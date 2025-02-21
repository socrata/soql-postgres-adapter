package com.socrata.pg.server

import scala.collection.{mutable => scm}

import java.io.{ByteArrayInputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, SQLException, PreparedStatement}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import javax.servlet.http.HttpServletResponse
import com.socrata.pg.BuildInfo
import com.rojoma.json.v3.ast.{JObject, JString, JValue, JArray, JNull}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonUtil, AutomaticJsonEncode}
import com.rojoma.simplearm.v2._
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.SimpleDocStream
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.soql.{SoQLTypeContext, SoQLRep}
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server._
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.implicits._
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.util.Precondition._
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.{EntityTag, NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.pg.analyzer2.CryptProviderProvider
import com.socrata.pg.SecondaryBase
import com.socrata.pg.query.{DataSqlizerQuerier, ExplainInfo, QueryResult, QueryServerHelper, RowCount, RowReaderQuerier}
import com.socrata.pg.server.config.{DynamicPortMap, QueryServerConfig}
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.soql._
import com.socrata.pg.store._
import com.socrata.soql.{BinaryTree, Compound, Leaf, SoQLAnalysis, typed}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.analyzer2.Statement
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.stdlib.{Context => SoQLContext}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.SoQLID.ClearNumberRep
import com.socrata.soql.types.{SoQLID, SoQLType, SoQLValue, SoQLVersion, SoQLNull}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.http.common.util.HttpUtils
import com.socrata.metrics.rollup.RollupMetrics
import com.socrata.metrics.rollup.events.RollupHit
import com.socrata.pg.server.CJSONWriter.utf8EncodingName
import com.socrata.thirdparty.metrics.{MetricsReporter, SocrataHttpSupport}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import java.time.{Clock, LocalDateTime}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.collection.immutable.SortedMap
import scala.language.existentials

class QueryServer(val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity, val leadingSearch: Boolean = true,
                  val httpQueryTimeoutDelta: FiniteDuration = Duration.Zero, val processQuery: analyzer2.ProcessQuery) extends SecondaryBase {
  import QueryServer._ // scalastyle:ignore import.grouping
  import QueryServerHelper._
  import com.socrata.pg.query.QueryResult._

  val dsConfig: DataSourceConfig = null // scalastyle:ignore null // unused

  val postgresUniverseCommon = PostgresUniverseCommon

  private val JsonContentType = "application/json; charset=utf-8"

  private val routerSet = locally {
    Routes(
      Route("/schema", SchemaResource),
      Route("/exists", ExistsResource),
      Route("/rollups", RollupResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource),
      Route("/info", InfoResource),
      Route("/new-query", NewQueryResource)
    )
  }

  private def route(req: HttpRequest): HttpResponse = {
    routerSet(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound
    }
  }

  object VersionResource extends SimpleResource {
    val response = OK ~> Content("application/json", BuildInfo.toJson)

    override val get = { _: HttpRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
    def schema(req: HttpRequest): HttpResponse = {
      val servReq = req.servletRequest
      val copy = Option(servReq.getParameter("copy"))
      val withFieldName = Option(servReq.getParameter("fieldName")).map(java.lang.Boolean.parseBoolean(_)).getOrElse(false)

      withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
        // get schema by dataset id or resource name
        val copyInfoOpt = Option(servReq.getParameter("ds")) match {
          case Some(ds) =>
            getCopy(pgu, ds, copy)
          case None => Option(servReq.getParameter("rn")) match {
            case Some(rn) =>
              getCopy(pgu, DatasetResourceName(rn), copy)
            case None =>
              None
          }
        }

        copyInfoOpt match {
          case Some(copyInfo) =>
            if (withFieldName) {
              import com.socrata.pg.ExtendedSchema._
              val schema =  getSchemaWithFieldName(pgu, copyInfo)
              OK ~>
              copyInfoHeaderForSchema(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified) ~>
              Write(JsonContentType)(JsonUtil.writeJson(_, schema, buffer = true))
            } else {
              import com.socrata.pg.Schema._
              val schema =  getSchema(pgu, copyInfo)
              OK ~>
              copyInfoHeaderForSchema(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified) ~>
              Write(JsonContentType)(JsonUtil.writeJson(_, schema, buffer = true))
            }
          case None => NotFound
        }
      }
    }
  }

  object ExistsResource extends SimpleResource {
    override val get = exists _
    def exists(req: HttpRequest): HttpResponse = {
      val servReq = req.servletRequest

      val ds = Option(servReq.getParameter("ds")).getOrElse(throw new IllegalArgumentException("Existence Checking requires the dataset parameter"))
      val copy = Option(servReq.getParameter("copy"))


      withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
        val copyInfoOpt = getCopy(pgu, ds, copy)


        copyInfoOpt match {
          case Some(copyInfo) => OK
          case None => NotFound
        }
      }
    }
  }

  object RollupResource extends SimpleResource {
    override val get = rollups _
    def rollups(req: HttpRequest): HttpResponse = {
      val servReq = req.servletRequest
      val dsOrRn = Option(servReq.getParameter("ds")).toLeft(servReq.getParameter("rn"))
      val copy = Option(servReq.getParameter("copy"))
      val includeUnmaterialized = java.lang.Boolean.parseBoolean(servReq.getParameter("include_unmaterialized"))
      getRollups(dsOrRn, copy, includeUnmaterialized) match {
        case Some(rollups) =>
          OK ~> Write(JsonContentType)(JsonUtil.writeJson(_, rollups.map(r => r.unanchored).toSeq, buffer = true))
        case None =>
          NotFound
      }
    }
  }

  object QueryResource extends SimpleResource {
    override val get = query(false) _
    override val post = query(false) _
  }

  object InfoResource extends SimpleResource {
    override val post = query(true) _
  }

  object NewQueryResource extends SimpleResource {
    override val post = newQuery _
  }

  def newQuery(req: HttpRequest): HttpResponse = {
    val rs = req.resourceScope

    val parsed = analyzer2.Deserializer(req.inputStream)

    processQuery(parsed, openPgu(dsInfo, None, rs), req.precondition, rs)
  }

  def etagFromCopy(datasetInternalName: String, copy: CopyInfo, etagInfo: Option[String], debug: Boolean = false): EntityTag = {
    // ETag is a hash based on datasetInternalName_copyNumber_version
    // Upstream components may pass through etag headers to and from query server but generate different queries.
    // For example, "select *" may become "select `a`" or "select `a`, `newly_unhidden_colujmn`"
    // Including query string using etagInfo in hash generation makes etags more robust.
    val etagInfoDigest = etagInfo.map { x =>
      val md = MessageDigest.getInstance("SHA1")
      md.update(x.getBytes(StandardCharsets.UTF_8))
      Base64.encodeBase64URLSafeString(md.digest())
    }.getOrElse("")
    val etagContents = s"${datasetInternalName}_${copy.copyNumber}_${copy.dataVersion}$etagInfoDigest"
    if(debug) logger.info(s"etagContents: $etagContents")
    StrongEntityTag(etagContents.getBytes(StandardCharsets.UTF_8))
  }

  def createEtagFromAnalysis(analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], fromTable: String, contextVars: SoQLContext): String = {
    val suffix =
      if(contextVars.nonEmpty) {
        JsonUtil.renderJson(contextVars.canonicalized, pretty = false)
      } else {
        ""
      }

    // Strictly speaking this table name is incorrect, but it's _consistently_ incorrect
    val etag = analyses match {
      case Compound(op, l, r) =>
        createEtagFromAnalysis(l, fromTable, SoQLContext.empty) + op + createEtagFromAnalysis(r, fromTable, SoQLContext.empty)
      case Leaf(analysis) =>
        if (analysis.from.isDefined) analysis.toString()
        else analysis.toStringWithFrom(TableName(fromTable))
    }
    etag + suffix
  }

  /** maps dataset name to count of current requests.  Requires synchronization to access. */
  private val currentRequests = scala.collection.mutable.HashMap[String,Integer]().withDefaultValue(0)

  /**
    * Limits the maximum concurrent requests for the given dataset.  Requests beyond the limit will return
    * a HTTP 429 Too Many Requests response.
    */
  private def withRequestLimit(datasetId: String, resp: HttpServletResponse, f: HttpServletResponse => Unit) = {
    val tooManyAlready = currentRequests.synchronized {
      val currentRequestsForDataset = currentRequests(datasetId)
      logger.trace(s"Currently processing $currentRequestsForDataset for dataset $datasetId, max allowed is ${QueryServer.config.maxConcurrentRequestsPerDataset}")
      if (currentRequestsForDataset >= QueryServer.config.maxConcurrentRequestsPerDataset) {
        true
      } else {
        currentRequests(datasetId) = currentRequestsForDataset + 1
        false
      }
    }

    if (tooManyAlready) {
      logger.warn(s"Rejecting request for dataset $datasetId, already processing limit of ${QueryServer.config.maxConcurrentRequestsPerDataset} concurrent requests per dataset")
      TooManyRequests(resp)
    } else {
      try {
        f(resp)
      } finally {
        currentRequests.synchronized {
          val currentRequestsForDataset = currentRequests(datasetId)
          if (currentRequestsForDataset <= 1) {
            currentRequests.remove(datasetId)
          } else {
            currentRequests(datasetId) = currentRequestsForDataset - 1
          }
        }
      }
    }
  }

  private def query(explain: Boolean)(req: HttpRequest)(resp: HttpServletResponse): Unit =  {
    val servReq = req.servletRequest
    val datasetId = servReq.getParameter("dataset")
    val analysisParam = Option(servReq.getParameter("query")) match {
      case Some(q) => q
      case _ =>
        using(servReq.getInputStream) { ins =>
          IOUtils.toString(ins, StandardCharsets.UTF_8.name)
        }
    }

    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = SoQLAnalyzerHelper.deserialize(analysisStream)
    val contextVars = Option(servReq.getParameter("context")).fold(SoQLContext.empty) { contextStr =>
      JsonUtil.parseJson[SoQLContext](contextStr) match {
        case Right(m) =>
          m
        case Left(_) =>
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          return
      }
    }

    val reqRowCount = Option(servReq.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    val copy = Option(servReq.getParameter("copy"))
    val rollupName = Option(servReq.getParameter("rollupName")).map(new RollupName(_))
    val obfuscateId = !Option(servReq.getParameter("obfuscateId")).exists(_ == "false")
    val timeoutMs = Option(servReq.getParameter("queryTimeoutSeconds")).map(_.toDouble * 1000).map(_.toLong)
    val queryTimeout = timeoutMs.map(new FiniteDuration(_, TimeUnit.MILLISECONDS))
    val debug = Option(servReq.getParameter("X-Socrata-Debug")).isDefined
    val analyze = Option(servReq.getHeader("X-Socrata-Analyze")).exists(_ == "true") && explain

    withRequestLimit(datasetId, resp,
      streamQueryResults(
        analyses = analyses,
        context = contextVars,
        datasetId = datasetId,
        reqRowCount = reqRowCount,
        copy = copy,
        rollupName = rollupName,
        obfuscateId = obfuscateId,
        precondition = req.precondition,
        ifModifiedSince = req.dateTimeHeader("If-Modified-Since"),
        givenLastModified = req.dateTimeHeader("X-Socrata-Last-Modified"),
        etagInfo = Option(createEtagFromAnalysis(analyses, datasetId, contextVars)),
        queryTimeout = queryTimeout,
        debug = debug,
        explain = explain,
        analyze = analyze))
  }

  def streamQueryResults( // scalastyle:ignore parameter.number
    analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
    context: SoQLContext,
    datasetId: String,
    reqRowCount: Boolean,
    copy: Option[String],
    rollupName: Option[RollupName],
    obfuscateId: Boolean,
    precondition: Precondition,
    ifModifiedSince: Option[DateTime],
    givenLastModified: Option[DateTime],
    etagInfo: Option[String],
    queryTimeout: Option[Duration],
    debug: Boolean,
    explain: Boolean,
    analyze: Boolean
  ) (resp:HttpServletResponse): Unit = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      pgu.datasetMapReader.datasetIdForInternalName(datasetId, checkDisabled = true) match {
        case None =>
          logger.info(s"Tried to perform query on dataset $datasetId")
          NotFound(resp)
        case Some(dsId) =>
          pgu.datasetMapReader.datasetInfo(dsId) match {
            case None =>
              logger.info(s"Tried to perform query on dataset $datasetId")
              NotFound(resp)
            case Some(datasetInfo) =>
              def notModified(etags: Seq[EntityTag]) = responses.NotModified ~> ETags(etags)
              def requestTimeout(timeout: Option[Duration]) = responses.RequestTimeout ~> Json(Map("timeout" -> timeout.toString))
              def invalidRequest(e: QueryError) = {
                responses.BadRequest ~> Json(QCRequestError(e.description))
              }
              execQuery(
                pgu = pgu,
                context = context,
                datasetInternalName = datasetId,
                datasetInfo = datasetInfo,
                analysis = analyses,
                rowCount = reqRowCount,
                reqCopy = copy,
                rollupName = rollupName,
                obfuscateId = obfuscateId,
                precondition = precondition,
                ifModifiedSince = ifModifiedSince,
                givenLastModified = givenLastModified,
                etagInfo = etagInfo,
                queryTimeout = queryTimeout,
                debug = debug,
                explain = explain,
                analyze = analyze) match {
                case NotModified(etags) => notModified(etags)(resp)
                case PreconditionFailed => responses.PreconditionFailed(resp)
                case RequestTimedOut(timeout) => requestTimeout(timeout)(resp)
                case Success(qrySchema, copyNumber, dataVersion, results, etag, lastModified, rollups) =>
                  // Very weird separation of concerns between execQuery and streaming. Most likely we will
                  // want yet-another-refactoring where much of execQuery is lifted out into this function.
                  // This will significantly change the tests; however.
                  rollupName.foreach(rollupName=>
                    //rollup name exists and we just successfully executed the query, lets tell someone
                    RollupMetrics.digest(RollupHit(datasetInfo.resourceName.underlying,rollupName.underlying,LocalDateTime.now(Clock.systemUTC())))
                  )
                  if(debug) logger.info(s"Returning etag: ${etag.asBytes.mkString(",")}")
                  ETag(etag)(resp)
                  copyInfoHeaderForRows(copyNumber, dataVersion, lastModified)(resp)
                  writeRollupHeader("X-SODA2-Rollup", rollupName, rollups)(resp)
                  results.run { r =>
                    CJSONWriter.writeCJson(datasetInfo, qrySchema,
                      r, reqRowCount, r.rowCount, dataVersion, lastModified, obfuscateId)(resp)
                  }
                case InfoSuccess(copyNumber, dataVersion, explainInfo) =>
                  implicit val encode = AutomaticJsonEncodeBuilder[ExplainInfo]
                  Header("X-SODA2-CopyNumber", copyNumber.toString)(resp)
                  Header("X-SODA2-DataVersion", dataVersion.toString)(resp)
                  Header("X-SODA2-Secondary-Last-Modified", DateTime.now().toHttpDate)(resp)
                  resp.setContentType("application/json")
                  resp.setCharacterEncoding(utf8EncodingName)

                  val writer = new OutputStreamWriter(resp.getOutputStream, StandardCharsets.UTF_8)
                  JsonUtil.writeJson(writer, Array(explainInfo))
                  writer.flush()
                  writer.close()
                case e: QueryError =>
                  invalidRequest(e)(resp)
              }
          }
      }
    }
  }

  private def writeRollupHeader(header: String, ru: Option[RollupName], rus: Seq[RollupInfo])(resp: HttpServletResponse): Unit = {
    val combined = ru.toSeq.map(_.underlying) ++
      rus.map(ru => s"${(new TableName(ru.copyInfo.datasetInfo.resourceName.underlying)).nameWithoutPrefix}.${ru.name.underlying}")
    if (combined.nonEmpty) {
      Header(header, combined.mkString(","))(resp)
    }
  }

  trait QueryResultBall
  case class RowsQueryResult(
    querySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
    dataVersion: Long,
    rows: Managed[CloseableIterator[Row[SoQLValue]] with RowCount],
    rollups: Seq[LocalRollupInfo]
  ) extends QueryResultBall
  case class InfoQueryResult(
    dataVersion: Long,
    infoMessage: ExplainInfo
  ) extends QueryResultBall

  def execQuery( // scalastyle:ignore method.length parameter.number cyclomatic.complexity
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    context: SoQLContext,
    datasetInternalName: String,
    datasetInfo: DatasetInfo,
    analysis: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
    rowCount: Boolean,
    reqCopy: Option[String],
    rollupName: Option[RollupName],
    obfuscateId: Boolean,
    precondition: Precondition,
    ifModifiedSince: Option[DateTime],
    givenLastModified: Option[DateTime],
    etagInfo: Option[String],
    queryTimeout: Option[Duration],
    debug: Boolean,
    explain: Boolean,
    analyze: Boolean
  ): QueryResult = {

    /**
     * TODO: Refactor QueryServer.runQuery to use QueryServerHelper.sqlize
     */
    def runQuery(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                 context: SoQLContext,
                 latestCopy: CopyInfo,
                 analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
                 rowCount: Boolean,
                 queryTimeout: Option[Duration],
                 explain: Boolean,
                 analyze: Boolean): QueryResultBall = {
      val cryptProvider = latestCopy.datasetInfo.cryptProvider

      val obfuscationKeySql = new Object {
        override lazy val toString =
          for {
            stmt <- managed(pgu.conn.prepareStatement("select make_obfuscator(?)"))
            .and(_.setBytes(1, cryptProvider.key))
            rs <- managed(stmt.executeQuery())
          } {
            if(!rs.next()) {
              throw new Exception("Selecting a single value didn't return one?")
            }
            rs.getBytes(1).map { b => "%02x".format(b & 0xff) }.mkString("bytea '\\x", "", "'")
          }
      }

      val sqlCtx = Map[SqlizerContext, Any](
        SqlizerContext.NameCache -> new Sqlizer.NameCache,
        SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
                                 else { new ClearNumberRep(cryptProvider) }),
        SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
        SqlizerContext.CaseSensitivity -> caseSensitivity,
        SqlizerContext.LeadingSearch -> leadingSearch,
        SqlizerContext.SoQLContext -> context,
        SqlizerContext.ObfuscationKeySql -> obfuscationKeySql
      )
      val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

      val lma = analyses.leftMost.leaf

      for(readCtx <- pgu.datasetReader.openDataset(latestCopy)) {
        val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
        val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
        val qrySchema = querySchema(pgu, analyses.outputSchema.leaf, latestCopy)
        val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor)
        val querier = readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
        val sqlReps = querier.getSqlReps(readCtx.copyInfo.dataTableName, systemToUserColumnMap)
        val typeReps = QueryServerHelper.typeReps

        // rollups will cause querier's dataTableName to be different than the normal dataset tablename
        val tableName = querier.sqlizer.dataTableName
        val copyInfo = readCtx.copyInfo

        // There are two kinds of related table entries -
        // 1. regular tablename contains dataset_map.resource_name
        // 2. rollup tablename contains dataset_map.resource_name + "." + rollup_map.name
        val relatedTableNames = removeTableAlias(collectRelatedTableNames(analysis))
        val (relatedCopyMap, relatedRollupMap) = getCopyAndRollupMaps(pgu, relatedTableNames, datasetInfo.resourceName, reqCopy)
        val joinCopiesMap = relatedCopyMap ++ Map(TableName(copyInfo.datasetInfo.resourceName.underlying) -> copyInfo)

        val sqlRepsWithJoin = relatedCopyMap.foldLeft(sqlReps) { (acc, joinCopy) =>
          val (tableName, copyInfo) = joinCopy
          acc ++ getJoinReps(pgu, copyInfo, tableName)
        }

        val thisTableNameMap = lma.from match {
          case Some(tn@(TableName(name, _))) if name == TableName.This =>
            Map(tn.copy(alias = None) -> tableName)
          case _ =>
            Map.empty
        }

        if (explain) {
          val explain = querier.queryExplain(
            context,
            analyses,
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = mapCopyToTablename(joinCopiesMap) ++ mapRollupToTablename(relatedRollupMap) + (TableName.PrimaryTable -> tableName)
              Sqlizer.sql((as, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            queryTimeout,
            debug,
            analyze)

          InfoQueryResult(latestCopy.dataVersion, explain)
        } else {
          val results = querier.query(
            context,
            analyses,
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = mapCopyToTablename(joinCopiesMap) ++ mapRollupToTablename(relatedRollupMap) ++ thisTableNameMap +
              (TableName.PrimaryTable -> tableName) + (TableName(tableName) -> tableName)
              Sqlizer.sql((as, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = mapCopyToTablename(joinCopiesMap) ++ mapRollupToTablename(relatedRollupMap) ++ thisTableNameMap + (TableName.PrimaryTable -> tableName) + (TableName(tableName) -> tableName)
              BinarySoQLAnalysisSqlizer.rowCountSql((as, tableNameMap, sqlReps.values.toSeq))(
                sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            rowCount,
            qryReps,
            queryTimeout,
            debug)

          RowsQueryResult(qrySchema, latestCopy.dataVersion, results, relatedRollupMap.values.toSeq)
        }
      }
    }

    val copy = getCopy(pgu, datasetInfo, reqCopy)
    if(debug) {
      logger.info(s"etagInfo: ${etagInfo.map(_.getBytes(StandardCharsets.ISO_8859_1).mkString(","))}")
      logger.info(s"datasetInternalName: $datasetInternalName")
      logger.info(s"copy: $copy")
    }
    val etag = etagFromCopy(datasetInternalName, copy, etagInfo, debug)
    if(debug) logger.info(s"Generated etag: ${etag.asBytes.mkString(",")}")
    val lastModified = givenLastModified match {
      case None => copy.lastModified
      case Some(d) => if (d.isAfter(copy.lastModified)) d else copy.lastModified
    }

    // Conditional GET handling
    precondition.check(Some(etag), sideEffectFree = true) match {
      case Passed =>
        ifModifiedSince match {
          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
                            && precondition == NoPrecondition =>
            NotModified(Seq(etag))
          case Some(_) | None =>
            val qto = queryTimeout.map(_.minus(httpQueryTimeoutDelta)) // can go negative which is handled by downstream function
            try {
              runQuery(pgu, context, copy, analysis, rowCount, qto, explain, analyze) match {
                case RowsQueryResult(qrySchema, version, results, rollups) =>
                  Success(qrySchema, copy.copyNumber, version, results, etag, lastModified, rollups)
                case InfoQueryResult(dataVersion, explainInfo) =>
                  InfoSuccess(copy.copyNumber, dataVersion, explainInfo)
              }
            } catch {
              case ex: SQLException =>
                QueryRuntimeError(ex) match {
                  case Some(error) => error
                  case None => throw ex
                }
              case ex: SqlizeError =>
                QueryError(ex.getMessage)
            }
        }
      case FailedBecauseMatch(etags) =>
        NotModified(etags)
      case FailedBecauseNoMatch =>
        PreconditionFailed
    }
  }

  /**
   * Get lastest schema
   * @param ds Data coordinator dataset id
   * @return Some schema or none
   */
  def getSchema(ds: String, reqCopy: Option[String]): Option[Schema] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetId <- pgu.datasetMapReader.datasetIdForInternalName(ds)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }

  def getSchema(id: DatasetResourceName, reqCopy: Option[String]): Option[Schema] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(id)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }

  def getSchemaWithFieldName(id: DatasetResourceName, reqCopy: Option[String]): Option[SchemaWithFieldName] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(id)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchemaWithFieldName(readCtx.copyCtx))
      }
    }
  }

  def getRollups(dsOrRn: Either[String, String], reqCopy: Option[String], includeUnmaterialized: Boolean): Option[Iterable[RollupInfo]] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      // get dataset by either id or resource name
      val datasetIdOption = dsOrRn match {
        case Left(ds) =>
          pgu.datasetMapReader.datasetIdForInternalName(ds, checkDisabled = true)
        case Right(rn) =>
          val datasetInfo = pgu.datasetMapReader.datasetInfoByResourceName(DatasetResourceName(rn))
          datasetInfo.map(_.systemId)
        case _ =>
          logger.warn("Require either dataset internal name or resource name to fetch rollups.  Possibly error from upstream Query Coordinator")
          None
      }

      for {
        datasetId <- datasetIdOption
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        if (includeUnmaterialized || RollupManager.shouldMaterializeRollups(copy.lifecycleStage)) {
          pgu.datasetMapReader.rollups(copy)
        } else {
          None
        }
      }
    }
  }

  private def getSchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copy: CopyInfo): Schema = {
    pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
  }

  private def getSchemaWithFieldName(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copy: CopyInfo): SchemaWithFieldName = {
    pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchemaWithFieldName(readCtx.copyCtx))
  }

  private def copyInfoHeaderForSchema(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    copyInfoHeader("Last-Modified")(copyNumber, dataVersion, lastModified)
  }

  private def copyInfoHeaderForRows(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    // TODO: Keeping "Last-Modified" just to make roll out easy.  It will be removed in the next cycle.
    copyInfoHeader("Last-Modified")(copyNumber, dataVersion, lastModified) ~>
      copyInfoHeader("X-SODA2-Secondary-Last-Modified")(copyNumber, dataVersion, lastModified)
  }

  private def copyInfoHeader(lastModifiedHeader: String)(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    Header(lastModifiedHeader, lastModified.toHttpDate) ~>
      Header("X-SODA2-CopyNumber", copyNumber.toString) ~>
      Header("X-SODA2-DataVersion", dataVersion.toString)
  }
}

object QueryServer extends DynamicPortMap {
  private val logger = Logger[QueryServer]

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if (ifaces.isEmpty) {
      config
    } else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soql-server-pg.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryServerConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.soql-server-pg")
  } catch {
    case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def main(args:Array[String]): Unit = {
    val address = config.discovery.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    implicit object executorResource extends com.rojoma.simplearm.v2.Resource[ExecutorService] {
      def close(a: ExecutorService): Unit = a.shutdown()
    }

    for {
      curator <- CuratorFromConfig(config.curator)
      discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
      pong <- managed(new LivenessCheckResponder(config.livenessCheck))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
      reporter <- MetricsReporter.managed(config.metrics)
      resultCache <- config.cache.makeCache
      timeoutManager <- managed(new analyzer2.BundledTimeoutManager(dsInfo.dataSource))
        .and(_.start())
    } {
      pong.start()
      val queryServer = new QueryServer(
        dsInfo,
        CaseSensitive,
        config.leadingSearch,
        config.httpQueryTimeoutDelta,
        new analyzer2.ProcessQuery(resultCache, timeoutManager)
      )
      val advertisedLivenessCheckInfo = new LivenessCheckInfo(hostPort(pong.livenessCheckInfo.getPort),
                                                              pong.livenessCheckInfo.getResponse)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(advertisedLivenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery,
                                            address,
                                            config.discovery.name + "." + config.instance,
                                            Some(auxData)) {
        override def register(port: Int): Cookie = {
          super.register(hostPort(port))
        }
      }
      val logOptions = LoggingOptions(LoggerFactory.getLogger(""),
                                      logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))
      val handler = ThreadRenamingHandler(NewLoggingHandler(logOptions)(queryServer.route))
      val server = new SocrataServerJetty(handler,
                     SocrataServerJetty.defaultOptions.
                       withGzipOptions(
                         Some(
                           SocrataServerJetty.Gzip.defaultOptions.
                             withExcludedMimeTypes(
                               Set("application/x-socrata-gzipped-cjson")
                             )
                         )
                       ).
                       withPort(config.port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                       withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
                       withBroker(curatorBroker))
      logger.info("starting pg query server")
      server.run()
    }
    logger.info("pg query server exited")
  }
}
