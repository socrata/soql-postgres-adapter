package com.socrata.pg.server

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
import com.rojoma.json.util.JsonUtil
import org.apache.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstanceBuilder}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetCopyContext, CopyInfo, Schema, ColumnInfo}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.{SimpleRouteContext, SimpleResource}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.util.handlers.{LoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server._
import com.socrata.pg.query.{DataSqlizerQuerier, RowReaderQuerier, RowCount}
import com.socrata.pg.server.config.QueryServerConfig
import com.socrata.pg.Schema._
import com.socrata.pg.{Version, SecondaryBase}
import com.socrata.pg.soql.{CaseSensitive, CaseSensitivity, SqlizerContext, Sqlizer}
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse, SchemaUtil, PGSecondaryRowReader}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.{SoQLVersion, SoQLValue, SoQLID, SoQLType}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.log4j.PropertyConfigurator
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Executors, ExecutorService}
import java.sql.Connection
import java.io.ByteArrayInputStream
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import scala.language.existentials
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.http.server.util.{NoPrecondition, Precondition, EntityTag, StrongEntityTag}
import com.socrata.http.server.util.Precondition._
import org.joda.time.DateTime


class QueryServer(val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity) extends SecondaryBase with Logging {
  import QueryServer._

  val dsConfig: DataSourceConfig = null // unused

  val postgresUniverseCommon = PostgresUniverseCommon

  private val routerSet = locally {
    import SimpleRouteContext._
    Routes(
      Route("/schema", SchemaResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource)
    )
  }

  private def route(req: HttpServletRequest): HttpResponse = {
    routerSet(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound
    }
  }

  object VersionResource extends SimpleResource {
    val response = OK ~> ContentType("application/json; charset=utf-8") ~> Content(JsonUtil.renderJson(Version("soql-server-pg")))

    override val get = { req: HttpServletRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
  }

  def schema(req: HttpServletRequest): HttpResponse = {
    val ds = req.getParameter("ds")
    latestSchema(ds) match {
      case Some(schema) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, schema, buffer = true))
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

  def query(req: HttpServletRequest): HttpServletResponse => Unit =  {

    val datasetId = req.getParameter("dataset")
    val analysisParam = req.getParameter("query")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val schemaHash = req.getParameter("schemaHash")
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.deserializer(analysisStream)
    val reqRowCount = Option(req.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    logger.info("Performing query on dataset " + datasetId)
    streamQueryResults(analysis, datasetId, reqRowCount, req.precondition, req.dateTimeHeader("If-Modified-Since"))
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
    precondition: Precondition,
    ifModifiedSince: Option[DateTime]
  ) (resp:HttpServletResponse) = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetId) match {
        case Some(dsId) =>
          pgu.datasetMapReader.datasetInfo(dsId) match {
            case Some(datasetInfo) =>
              def notModified(etags: Seq[EntityTag]) = responses.NotModified ~> ETags(etags)

              execQuery(pgu, datasetId, datasetInfo, analysis, reqRowCount, precondition, ifModifiedSince) match {
                case Success(qrySchema, dataVersion, results, etag, lastModified) =>
                  // Very weird separation of concerns between execQuery and streaming. Most likely we will
                  // want yet-another-refactoring where much of execQuery is lifted out into this function.
                  // This will significantly change the tests; however.
                  ETag(etag)(resp)
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

      for (readCtx <- pgu.datasetReader.openDataset(latestCopy)) yield {
        val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
        val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
        val userToSystemColumnMap = SchemaUtil.userToSystemColumnMap(readCtx.schema)
        val qrySchema = querySchema(pgu, analysis, latestCopy)
        val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor(_))
        val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema)
        val sqlReps = querier.getSqlReps(systemToUserColumnMap)

        val results = querier.query(
          analysis,
          (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
            (a, tableName, sqlReps.values.toSeq).sql(sqlReps, Seq.empty, sqlCtx),
          (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
            (a, tableName, sqlReps.values.toSeq).rowCountSql(sqlReps, Seq.empty, sqlCtx),
          rowCount,
          systemToUserColumnMap,
          userToSystemColumnMap,
          qryReps)
        (qrySchema, latestCopy.dataVersion, results)
      }
    }

    val latest: CopyInfo = pgu.datasetMapReader.latest(datasetInfo)
    val etag = etagFromCopy(datasetInternalName, latest)
    val lastModified = latest.lastModified

    // Conditional GET handling
    precondition.check(Some(etag), sideEffectFree = true) match {
      case Passed =>
        ifModifiedSince match {
          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
                            && precondition == NoPrecondition =>
            NotModified(Seq(etag))
          case Some(_) | None =>
            val (qrySchema, version, results) = runQuery(pgu, latest, analysis, rowCount)
            Success(qrySchema, version, results, etag, lastModified)
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
                                                   schema: com.socrata.datacoordinator.util.collection.ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]]):
    PGSecondaryRowReader[SoQLType, SoQLValue] with RowReaderQuerier[SoQLType, SoQLValue] = {

    new PGSecondaryRowReader[SoQLType, SoQLValue] (
      conn,
      new PostgresRepBasedDataSqlizer(copyCtx.copyInfo.dataTableName, pgu.datasetContextFactory(schema), pgu.commonSupport.copyInProvider) with DataSqlizerQuerier[SoQLType, SoQLValue],
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
                  OrderedMap[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT]] = {

    analysis.selection.foldLeft(OrderedMap.empty[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new com.socrata.datacoordinator.id.ColumnId(map.size + 1)
          val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT](
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
  def latestSchema(ds: String): Option[Schema] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val latest = pgu.datasetMapReader.latest(datasetInfo)
        pgu.datasetReader.openDataset(latest).map(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }
}

object QueryServer extends Logging {

  sealed abstract class QueryResult
  case class NotModified(etags: Seq[EntityTag]) extends QueryResult
  case object PreconditionFailed extends QueryResult
  case class Success(
    qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
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

    val address = config.advertisement.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    implicit object executorResource extends com.rojoma.simplearm.Resource[ExecutorService] {
      def close(a: ExecutorService) { a.shutdown() }
    }

    for {
      curator <- managed(CuratorFrameworkFactory.builder.
        connectString(config.curator.ensemble).
        sessionTimeoutMs(config.curator.sessionTimeout.toMillis.toInt).
        connectionTimeoutMs(config.curator.connectTimeout.toMillis.toInt).
        retryPolicy(new retry.BoundedExponentialBackoffRetry(config.curator.baseRetryWait.toMillis.toInt,
        config.curator.maxRetryWait.toMillis.toInt,
        config.curator.maxRetries)).
        namespace(config.curator.namespace).
        build())
      discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
        client(curator).
        basePath(config.advertisement.basePath).
        build())
      pong <- managed(new LivenessCheckResponder(new InetSocketAddress(InetAddress.getByName(address), 0)))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      curator.start()
      discovery.start()
      pong.start()
      val queryServer = new QueryServer(dsInfo, CaseSensitive)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery, address, config.advertisement.name + "." + config.instance, Some(auxData))
      val handler = ThreadRenamingHandler(LoggingHandler(queryServer.route))
      val server = new SocrataServerJetty(handler, port = config.port, broker = curatorBroker)
      logger.info("starting pg query server")
      server.run()
    }
    logger.info("pg query server exited")
  }
}
