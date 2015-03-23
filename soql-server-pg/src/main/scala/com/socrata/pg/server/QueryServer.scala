package com.socrata.pg.server

import java.io.ByteArrayInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors}

import scala.language.existentials

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.id.{RollupName, ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server._
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.implicits._
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.{SimpleResource, SimpleRouteContext}
import com.socrata.http.server.util.{EntityTag, NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.http.server.util.Precondition._
import com.socrata.http.server.util.handlers.{NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.pg.{SecondaryBase, Version}
import com.socrata.pg.Schema._
import com.socrata.pg.query.{DataSqlizerQuerier, RowCount, RowReaderQuerier}
import com.socrata.pg.server.config.QueryServerConfig
import com.socrata.pg.soql.{CaseSensitive, CaseSensitivity, Sqlizer, SqlizerContext}
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.store._
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLID, SoQLType, SoQLValue, SoQLVersion}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.thirdparty.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.thirdparty.metrics.{SocrataHttpSupport, Metrics, MetricsReporter}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.Logging
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime

class QueryServer(val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity) extends SecondaryBase with Logging {
  import QueryServer._

  val dsConfig: DataSourceConfig = null // unused

  val postgresUniverseCommon = PostgresUniverseCommon

  private val JsonContentType = "application/json; charset=utf-8"

  private val routerSet = locally {
    import SimpleRouteContext._
    Routes(
      Route("/schema", SchemaResource),
      Route("/rollups", RollupResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource)
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
    val response = OK ~> Json(Version("soql-server-pg"))

    override val get = { _: HttpRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
  }

  def schema(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val ds = servReq.getParameter("ds")
    val copy = Option(servReq.getParameter("copy"))
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      getCopy(pgu, ds, copy) match {
        case Some(copyInfo) =>
          val schema = getSchema(pgu, copyInfo)
          OK ~>
            copyInfoHeader(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified) ~>
            Write(JsonContentType)(JsonUtil.writeJson(_, schema, buffer = true))
        case None =>
          NotFound
      }
    }
  }

  object RollupResource extends SimpleResource {
    override val get = rollups _
  }

  def rollups(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val ds = servReq.getParameter("ds")
    val copy = Option(servReq.getParameter("copy"))
    val includeUnmaterialized = java.lang.Boolean.parseBoolean(servReq.getParameter("include_unmaterialized"))
    getRollups(ds, copy, includeUnmaterialized) match {
      case Some(rollups) =>
        OK ~> Write(JsonContentType)(JsonUtil.writeJson(_, rollups.map(r => r.unanchored).toSeq, buffer = true))
      case None =>
        NotFound
    }
  }

  object QueryResource extends SimpleResource {
    override val get = query _
    override val post = query _
  }

  def etagFromCopy(datasetInternalName: String, copy: CopyInfo): EntityTag = {
    // ETag is a hash based on datasetInternalName_copyNumber_version
    val etagContents = s"${datasetInternalName}_${copy.copyNumber}_${copy.dataVersion}"
    StrongEntityTag(etagContents.getBytes(StandardCharsets.UTF_8))
  }

  def query(req: HttpRequest): HttpServletResponse => Unit =  {
    val servReq = req.servletRequest
    val datasetId = servReq.getParameter("dataset")
    val analysisParam = servReq.getParameter("query")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val schemaHash = servReq.getParameter("schemaHash")
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.deserializer(analysisStream)
    val reqRowCount = Option(servReq.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    val copy = Option(servReq.getParameter("copy"))
    val rollupName = Option(servReq.getParameter("rollupName")).map(new RollupName(_))

    logger.info("Performing query on dataset " + datasetId)
    streamQueryResults(analysis, datasetId, reqRowCount, copy, rollupName, req.precondition, req.dateTimeHeader("If-Modified-Since"))
  }

  /**
   * Stream the query results; we need to have the entire HttpServletResponse => Unit
   * passed back to SocrataHttp so the transaction can be maintained through the duration of the
   * streaming.
   */
  def streamQueryResults(
    analysis: SoQLAnalysis[UserColumnId, SoQLType],
    datasetId:String,
    reqRowCount: Boolean,
    copy: Option[String],
    rollupName: Option[RollupName],
    precondition: Precondition,
    ifModifiedSince: Option[DateTime]
  ) (resp:HttpServletResponse) = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetId) match {
        case Some(dsId) =>
          pgu.datasetMapReader.datasetInfo(dsId) match {
            case Some(datasetInfo) =>
              def notModified(etags: Seq[EntityTag]) = responses.NotModified ~> ETags(etags)

              execQuery(pgu, datasetId, datasetInfo, analysis, reqRowCount, copy, rollupName, precondition, ifModifiedSince) match {
                case Success(qrySchema, copyNumber, dataVersion, results, etag, lastModified) =>
                  // Very weird separation of concerns between execQuery and streaming. Most likely we will
                  // want yet-another-refactoring where much of execQuery is lifted out into this function.
                  // This will significantly change the tests; however.
                  ETag(etag)(resp)
                  copyInfoHeader(copyNumber, dataVersion, lastModified)(resp)
                  rollupName.foreach(r => Header("X-SODA2-Rollup", r.underlying)(resp))
                  for (r <- results) yield {
                    CJSONWriter.writeCJson(datasetInfo, qrySchema, r, reqRowCount, r.rowCount, dataVersion, lastModified)(resp)
                  }
                case NotModified(etags) =>
                  notModified(etags)(resp)
                case PreconditionFailed =>
                  responses.PreconditionFailed(resp)
              }
            case None =>
              NotFound(resp)
          }
        case None =>
          NotFound(resp)
      }
    }
  }

  def execQuery(
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    datasetInternalName: String,
    datasetInfo: DatasetInfo,
    analysis: SoQLAnalysis[UserColumnId, SoQLType],
    rowCount: Boolean,
    reqCopy: Option[String],
    rollupName: Option[RollupName],
    precondition: Precondition,
    ifModifiedSince: Option[DateTime]
  ): QueryResult = {
    import Sqlizer._

    def runQuery(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], latestCopy: CopyInfo, analysis: SoQLAnalysis[UserColumnId, SoQLType], rowCount: Boolean) = {
      val cryptProvider = new CryptProvider(latestCopy.datasetInfo.obfuscationKey)
      val sqlCtx = Map[SqlizerContext, Any](
        SqlizerContext.IdRep -> new SoQLID.StringRep(cryptProvider),
        SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
        SqlizerContext.CaseSensitivity -> caseSensitivity
      )
      val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

      for (readCtx <- pgu.datasetReader.openDataset(latestCopy)) yield {
        val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
        val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
        val qrySchema = querySchema(pgu, analysis, latestCopy)
        val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor(_))
        val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
        val sqlReps = querier.getSqlReps(systemToUserColumnMap)

        val results = querier.query(
          analysis,
          (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
            (a, tableName, sqlReps.values.toSeq).sql(sqlReps, Seq.empty, sqlCtx, escape),
          (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
            (a, tableName, sqlReps.values.toSeq).rowCountSql(sqlReps, Seq.empty, sqlCtx, escape),
          rowCount,
          qryReps)
        (qrySchema, latestCopy.dataVersion, results)
      }
    }

    val copy = getCopy(pgu, datasetInfo, reqCopy)
    val etag = etagFromCopy(datasetInternalName, copy)
    val lastModified = copy.lastModified

    // Conditional GET handling
    precondition.check(Some(etag), sideEffectFree = true) match {
      case Passed =>
        ifModifiedSince match {
          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
                            && precondition == NoPrecondition =>
            NotModified(Seq(etag))
          case Some(_) | None =>
            val (qrySchema, version, results) = runQuery(pgu, copy, analysis, rowCount)
            Success(qrySchema, copy.copyNumber, version, results, etag, lastModified)
        }
      case FailedBecauseMatch(etags) =>
        NotModified(etags)
      case FailedBecauseNoMatch =>
        PreconditionFailed
    }
  }

  private def readerWithQuery[SoQLType, SoQLValue](conn: Connection,
                                                   pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                                   copyCtx: DatasetCopyContext[SoQLType],
                                                   schema: ColumnIdMap[ColumnInfo[SoQLType]],
                                                   rollupName: Option[RollupName]):
    PGSecondaryRowReader[SoQLType, SoQLValue] with RowReaderQuerier[SoQLType, SoQLValue] = {

    val tableName = rollupName match {
      case Some(r) =>
        val rollupInfo = pgu.datasetMapReader.rollup(copyCtx.copyInfo, r).getOrElse {
          throw new RuntimeException(s"Rollup ${rollupName} not found for copy ${copyCtx.copyInfo} ")
        }
        RollupManager.rollupTableName(rollupInfo, copyCtx.copyInfo.dataVersion)
      case None =>
        copyCtx.copyInfo.dataTableName
    }

    new PGSecondaryRowReader[SoQLType, SoQLValue] (
      conn,
      new PostgresRepBasedDataSqlizer(tableName, pgu.datasetContextFactory(schema), pgu.commonSupport.copyInProvider) with DataSqlizerQuerier[SoQLType, SoQLValue],
      pgu.commonSupport.timingReport) with RowReaderQuerier[SoQLType, SoQLValue]
  }

  /**
   * @param pgu
   * @param analysis parsed soql
   * @param latest
   * @return a schema for the selected columns
   */
  // TODO: Handle expressions and column aliases.
  private def querySchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                  analysis: SoQLAnalysis[UserColumnId, SoQLType],
                  latest: CopyInfo):
                  OrderedMap[ColumnId, ColumnInfo[pgu.CT]] = {

    analysis.selection.foldLeft(OrderedMap.empty[ColumnId, ColumnInfo[pgu.CT]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new ColumnId(map.size + 1)
          val cinfo = new ColumnInfo[pgu.CT](
            latest,
            cid,
            new UserColumnId(columnName.name),
            coreExpr.typ,
            columnName.name,
            coreExpr.typ == SoQLID,
            false, // isUserKey
            coreExpr.typ == SoQLVersion
          )(SoQLTypeContext.typeNamespace, null)
          map + (cid -> cinfo)
      }
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
        datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).map(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }

  def getRollups(ds: String, reqCopy: Option[String], includeUnmaterialized: Boolean): Option[Iterable[RollupInfo]] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds)
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
    pgu.datasetReader.openDataset(copy).map(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], ds: String, reqCopy: Option[String])
                      : Option[CopyInfo] = {
    for {
      datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds)
      datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      getCopy(pgu, datasetInfo, reqCopy)
    }
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: DatasetInfo, reqCopy: Option[String]): CopyInfo = {
    val intRx = "^[0-9]+$".r
    val rd = pgu.datasetMapReader
    reqCopy match {
      case Some("latest") =>
        rd.latest(datasetInfo)
      case Some("published") | None =>
        rd.published(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some("unpublished") | None =>
        rd.unpublished(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some(intRx(num)) =>
        rd.copyNumber(datasetInfo, num.toLong).getOrElse(rd.latest(datasetInfo))
      case Some(unknown) =>
        throw new IllegalArgumentException(s"invalid copy value $unknown")
    }
  }

  private def copyInfoHeader(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    Header("Last-Modified", lastModified.toHttpDate) ~>
      Header("X-SODA2-CopyNumber", copyNumber.toString) ~>
      Header("X-SODA2-DataVersion", dataVersion.toString)
  }
}

object QueryServer extends Logging {

  sealed abstract class QueryResult
  case class NotModified(etags: Seq[EntityTag]) extends QueryResult
  case object PreconditionFailed extends QueryResult
  case class Success(
    qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
    copyNumber: Long,
    dataVersion: Long,
    results: Managed[CloseableIterator[Row[SoQLValue]] with RowCount],
    etag: EntityTag,
    lastModified: DateTime
  ) extends QueryResult


  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soql-server-pg.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryServerConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.soql-server-pg")
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def main(args:Array[String]) {

    val address = config.discovery.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    implicit object executorResource extends com.rojoma.simplearm.Resource[ExecutorService] {
      def close(a: ExecutorService) { a.shutdown() }
    }

    for {
      curator <- CuratorFromConfig(config.curator)
      discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
      pong <- managed(new LivenessCheckResponder(new InetSocketAddress(InetAddress.getByName(address), 0)))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
      reporter <- MetricsReporter.managed(config.metrics)
    } {
      pong.start()
      val queryServer = new QueryServer(dsInfo, CaseSensitive)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery,
                                            address,
                                            config.discovery.name + "." + config.instance,
                                            Some(auxData))
      val logOptions = NewLoggingHandler.defaultOptions.copy(
                         logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))
      val handler = ThreadRenamingHandler(NewLoggingHandler(logOptions)(queryServer.route))
      val server = new SocrataServerJetty(handler,
                     SocrataServerJetty.defaultOptions.
                       withPort(config.port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                       withBroker(curatorBroker))
      logger.info("starting pg query server")
      server.run()
    }
    logger.info("pg query server exited")
  }
}
