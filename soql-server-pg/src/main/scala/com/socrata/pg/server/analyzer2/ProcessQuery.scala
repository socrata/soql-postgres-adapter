package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}
import scala.concurrent.duration.FiniteDuration

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, SQLException, ResultSet}
import java.time.{Clock, LocalDateTime}
import java.util.Base64
import java.util.zip.GZIPOutputStream

import com.rojoma.json.v3.ast.{JArray, JString, JValue, JNull}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.json.v3.util.{AutomaticJsonEncode, JsonUtil}
import com.rojoma.simplearm.v2._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.{SimpleDocStream, SimpleDocTree, tree => doctree}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, Provenance, ScopedResourceName}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLID, SoQLVersion, SoQLNull}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.sql.Debug
import com.socrata.soql.sqlizer.{Sqlizer, ResultExtractor, SqlizeAnnotation, SqlizerUniverse, SqlNamespaces}
import com.socrata.soql.stdlib.analyzer2.SoQLRewritePassHelpers
import com.socrata.soql.stdlib.analyzer2.rollup.SoQLRollupExact
import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.json.JsonColumnWriteRep
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.metrics.rollup.RollupMetrics
import com.socrata.metrics.rollup.events.RollupHit

import com.socrata.pg.analyzer2.{CryptProviderProvider, TransformManager, CostEstimator, CryptProvidersByDatabaseNamesProvenance, RewriteSubcolumns, Namespaces, SoQLExtraContext}
import com.socrata.pg.analyzer2.metatypes.{CopyCache, InputMetaTypes, DatabaseMetaTypes, DatabaseNamesMetaTypes, AugmentedTableName, SoQLMetaTypesExt}
import com.socrata.pg.store.{PGSecondaryUniverse, SqlUtils, RollupAnalyzer, RollupId}
import com.socrata.pg.query.QueryResult
import com.socrata.pg.server.CJSONWriter
import com.socrata.datacoordinator.common.DbType
import com.socrata.datacoordinator.common.Postgres
import com.socrata.pg.analyzer2.SoQLSqlizer

final abstract class ProcessQuery
object ProcessQuery {
  val log = LoggerFactory.getLogger(classOf[ProcessQuery])

  def apply(
    request: Deserializer.Request,
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    precondition: Precondition,
    rs: ResourceScope
  ): HttpResponse = {
    val analysis = request.analysis
    val passes = request.passes
    val systemContext = request.context

    locally {
      import InputMetaTypes.DebugHelper._
      implicit def cpp = CryptProviderProvider.empty
      log.debug("Received analysis statement:\n{}", Lazy(analysis.statement.debugStr))
    }

    // ok, we have our analysis which is in terms of internal names
    // and column ids, and we want to turn that into having them in
    // terms of copies and columns
    val copyCache = new InputMetaTypes.CopyCache(pgu)

    val dmtState = new DatabaseMetaTypes
    val metadataAnalysis = dmtState.rewriteFrom(analysis, copyCache, InputMetaTypes.provenanceMapper)

    // Then we'll rewrite that analysis in terms of actual physical
    // database names, after building our crypt providers.
    implicit val cryptProviders: CryptProviderProvider = CryptProvidersByDatabaseNamesProvenance(metadataAnalysis.statement)

    // but first. produce a string of the analysis for etagging purposes
    val stringifiedAnalysis = locally {
      import InputMetaTypes.DebugHelper._
      analysis.statement.debugDoc.layoutPretty(LayoutOptions(PageWidth.Unbounded)).toString
    }

    val relevantRollups = copyCache.allCopies.
      flatMap { copy =>
        pgu.datasetMapReader.rollups(copy).
          iterator.
          filter { rollup => rollup.isNewAnalyzer && tableExists(pgu, rollup.tableName) }.
          flatMap { rollup => RollupAnalyzer(pgu, copy, rollup).map((rollup, _)) }.
          map { case (rollup, (_foundTables, analysis, _locationSubcolumnsMap, _cryptProvider)) =>
            // don't care about the other two things because:
            //   * we're not going to be sqlizing this rollup
            //   * any referenced datasets we already know about
            new RollupInfo[DatabaseNamesMetaTypes, RollupId] with QueryServerRollupInfo {
              override val id = rollup.systemId
              override val statement = analysis.statement
              override val resourceName = ScopedResourceName(-1, ResourceName(s"rollup:${rollup.copyInfo.datasetInfo.systemId}/${rollup.copyInfo.systemId}/${rollup.name.underlying}"))
              override val databaseName = DatabaseTableName(AugmentedTableName(rollup.tableName, isRollup = true))

              // Needed for metrics
              override val rollupDatasetName = rollup.copyInfo.datasetInfo.resourceName
              override val rollupName = rollup.name

              private val columns = statement.schema.keysIterator.toArray
              override def databaseColumnNameOfIndex(idx: Int) =
                DatabaseColumnName(Namespaces.rawAutoColumnBase(columns(idx)))
            }
          }
      }

    // Before doing rollups, extract our final hints (rollups can
    // destroy hints).
    val hintsByName = metadataAnalysis.statement.schema.valuesIterator.flatMap { schemaEnt =>
      schemaEnt.hint.map(schemaEnt.name -> _)
    }.toMap
    log.debug("Result column hints: {}", Lazy(JsonUtil.renderJson(hintsByName, pretty = true)))

    // Intermixed rewrites and rollups: rollups are attemted before
    // each group of rewrite passes and after all rewrite passes have
    // happened.
    val nameAnalyses = locally {
      import DatabaseNamesMetaTypes.DebugHelper._

      TransformManager[DatabaseNamesMetaTypes](
        DatabaseNamesMetaTypes.rewriteFrom(dmtState, metadataAnalysis),
        relevantRollups,
        passes,
        new SoQLRewritePassHelpers,
        new SoQLRollupExact(Stringifier.pretty),
        Stringifier.pretty
      )
    }

    case class Sqlized(
      analysis: SoQLAnalysis[DatabaseNamesMetaTypes],
      rollups: Seq[QueryServerRollupInfo],
      sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
      extractor: ResultExtractor[DatabaseNamesMetaTypes],
      nonliteralSystemContextLookupFound: Boolean,
      now: Option[DateTime],
      estimatedCost: Double
    )

    val onlyOne = nameAnalyses.lengthCompare(1) == 0
    val sqlized = nameAnalyses.map { nameAnalysis =>
      val sqlizer = SoQLSqlizer
      val Sqlizer.Result(sql, extractor, SoQLExtraContext.Result(nonliteralSystemContextLookupFound, now)) = sqlizer(nameAnalysis._1, new SoQLExtraContext(systemContext, cryptProviders, RewriteSubcolumns[InputMetaTypes](request.locationSubcolumns, copyCache), SqlUtils.escapeString(pgu.conn, _))) match {
        case Right(result) => result
        case Left(nothing) => nothing
      }
      log.debug("Generated sql:\n{}", sql) // Doc's toString defaults to pretty-printing

      Sqlized(
        nameAnalysis._1,
        relevantRollups.filter { rr => nameAnalysis._2.contains(rr.id) },
        sql,
        extractor,
        nonliteralSystemContextLookupFound,
        now,
        if(onlyOne) 0.0 else CostEstimator(pgu.conn, sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded)).toString)
      )
    }.minBy(_.estimatedCost)

    // Our etag will be the hash of the inputs that affect the result of the query
    val etag = ETagify(
      analysis.statement.schema.valuesIterator.map(_.name).toSeq,
      hintsByName,
      stringifiedAnalysis,
      copyCache.orderedVersions,
      systemContext,
      request.locationSubcolumns,
      passes,
      request.debug,
      sqlized.now
    )

    // "last modified" is problematic because it doesn't include
    // modification times of saved views.  At least, at higher levels
    // that actually know about saved views as things that exist and
    // can change over time.
    val lastModified = copyCache.mostRecentlyModifiedAt.getOrElse(new DateTime(0L))

    precondition.check(Some(etag), sideEffectFree = true) match {
      case Precondition.Passed =>
        fulfillRequest(
          hintsByName,
          if(sqlized.nonliteralSystemContextLookupFound) Some(systemContext) else None,
          copyCache,
          etag,
          pgu,
          sqlized.sql,
          sqlized.analysis,
          sqlized.rollups,
          cryptProviders,
          sqlized.extractor,
          lastModified,
          request.debug,
          request.queryTimeout,
          rs
        )
      case Precondition.FailedBecauseMatch(_) =>
        log.debug("if-none-match resulted in a cache hit")
        NotModified ~> ETag(etag) ~> LastModified(lastModified)
      case Precondition.FailedBecauseNoMatch =>
        log.debug("if-match resulted in a miss")
        PreconditionFailed ~> ETag(etag)
    }
  }

  trait QueryServerRollupInfo {
    val rollupDatasetName: Option[String]
    val rollupName: RollupName
  }

  def tableExists(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], table: String): Boolean = {
    for {
      stmt <- managed(pgu.conn.prepareStatement("SELECT exists(select 1 from pg_tables where schemaname = 'public' and tablename = ?)")).
        and(_.setString(1, table))
      rs <- managed(stmt.executeQuery())
    } {
      if(!rs.next()) {
        throw new Exception("`SELECT exists(..)` returned nothing??")
      }
      rs.getBoolean(1)
    }
  }

  def setTimeout(conn: Connection, timeout: FiniteDuration): Unit = {
    val timeoutMs = timeout.toMillis.min(Int.MaxValue).max(1).toInt
    using(conn.createStatement()) { stmt =>
      stmt.execute(s"SET LOCAL statement_timeout TO $timeoutMs")
    }
  }

  def resetTimeout(conn: Connection): Unit = {
    using(conn.createStatement()) { stmt =>
      stmt.execute("SET LOCAL statement_timeout TO DEFAULT")
    }
  }

  // note this _doesn't_ reset the timeout if it throws
  def withTimeout[T](conn: Connection, timeout: Option[FiniteDuration], cond: Boolean = true)(f: => T): T = {
    timeout match {
      case Some(finite) if cond =>
        setTimeout(conn, finite)
        val result = f
        resetTimeout(conn)
        result
      case _ =>
        f
    }
  }

  def explainText(
    conn: Connection,
    query: String,
    analyze: Boolean,
    timeout: Option[FiniteDuration]
  ): Either[HttpResponse, String] = {
    handlingSqlErrors(timeout) {
      withTimeout(conn, timeout, cond = analyze) {
        for {
          stmt <- managed(conn.createStatement())
          rs <- managed(stmt.executeQuery(s"EXPLAIN (analyze $analyze, format text) $query"))
        } {
          val sb = new StringBuilder
          var didOne = false
          while(rs.next()) {
            if(didOne) sb.append('\n')
            else didOne = true
            sb.append(rs.getString(1))
          }
          sb.toString
        }
      }
    }
  }

  def explainJson(
    conn: Connection,
    query: String,
    analyze: Boolean,
    timeout: Option[FiniteDuration]
  ): Either[HttpResponse, JValue] = {
    handlingSqlErrors(timeout) {
      withTimeout(conn, timeout, cond = analyze) {
        for {
          stmt <- managed(conn.createStatement())
          rs <- managed(stmt.executeQuery(s"EXPLAIN (analyse $analyze, format json) $query"))
        } {
          val sb = new StringBuilder
          var didOne = false
          if(!rs.next()) {
            throw new Exception("Should've gotten a row for the explanation")
          }
          JsonReader.fromString(rs.getString(1))
        }
      }
    }
  }

  def handlingSqlErrors[T](timeout: Option[FiniteDuration])(f: => T): Either[HttpResponse, T] = {
    try {
      Right(f)
    } catch {
      case e: SQLException =>
        QueryResult.QueryRuntimeError(e, timeout) match {
          case Some(QueryResult.RequestTimedOut(timeout)) =>
            Left(RequestTimeout ~> Json(json"{ timeout: ${timeout.toString} }"))
          case Some(QueryResult.QueryError(description)) =>
            // Sadness: PG _doesn't_ report position info for runtime
            // errors so this position info isn't actually useful,
            // even if the sql were available here...
            // val posInfo = Sqlizer.positionInfo(laidOutSql)

            Left(BadRequest ~> Json(json"{ message: $description }"))
          case None =>
            log.error("Exception running query", e)
            Left(InternalServerError)
        }
    }
  }

  def fulfillRequest(
    hintsByName: Map[ColumnName, JValue],
    systemContext: Option[Map[String, String]],
    copyCache: CopyCache[InputMetaTypes],
    etag: EntityTag,
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
    nameAnalysis: com.socrata.soql.analyzer2.SoQLAnalysis[DatabaseNamesMetaTypes],
    rollups: Seq[QueryServerRollupInfo],
    cryptProviders: CryptProviderProvider,
    extractor: ResultExtractor[DatabaseNamesMetaTypes],
    lastModified: DateTime,
    debug: Option[Debug],
    timeout: Option[FiniteDuration],
    rs: ResourceScope
  ): HttpResponse = {
    val locale = "en_US"

    val laidOutSql: SimpleDocStream[SqlizeAnnotation[DatabaseNamesMetaTypes]] = sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    val renderedSql = laidOutSql.toString
    val debugFields =
      debug.map { debug =>
        Seq(
          debug.sql.map { fmt =>
            val s =
              fmt match {
                case Debug.Sql.Format.Compact =>
                  renderedSql
                case Debug.Sql.Format.Pretty =>
                  prettierSql(nameAnalysis, sql)
              }
            JString(s)
          }.map("sql" -> _),
          debug.explain.map { case Debug.Explain(analyze, format) =>
            val explainResult =
              format match {
                case Debug.Explain.Format.Text =>
                  explainText(pgu.conn, renderedSql, analyze, timeout).map(JString(_))
                case Debug.Explain.Format.Json =>
                  explainJson(pgu.conn, renderedSql, analyze, timeout)
              }
            explainResult match {
              case Left(errorResponse) => return errorResponse
              case Right(json) => json
            }
          }.map("explain" -> _)
        ).flatten.toMap
      }

    val preventRun = debug.fold(false)(_.inhibitRun)

    val rows =
      if(preventRun) {
        Iterator.empty
      } else {
        timeout.foreach(setTimeout(pgu.conn, _))
        handlingSqlErrors(timeout) {
          runQuery(pgu.conn, renderedSql, systemContext, cryptProviders, extractor, rs)
        } match {
          case Left(resp) => return resp
          case Right(rows) => rows
        }
      }

    if(!preventRun) {
      for(ri <- rollups) {
        RollupMetrics.digest(
          RollupHit(
            ri.rollupDatasetName.getOrElse("unknown"),
            ri.rollupName.underlying,
            LocalDateTime.now(Clock.systemUTC())
          )
        )
      }
    }

    ETag(etag) ~> LastModified(lastModified) ~> ContentType("application/x-socrata-gzipped-cjson") ~> Stream { rawOutputStream =>
      for {
        gzos <- managed(new GZIPOutputStream(rawOutputStream))
        osw <- managed(new OutputStreamWriter(gzos, StandardCharsets.UTF_8))
        writer <- managed(new BufferedWriter(osw))
      } {
        writer.write("[{")
        for(debugFields <- debugFields) {
          writer.write("\"debug\":%s\n ,".format(JsonUtil.renderJson(debugFields, pretty = false)))
        }
        writer.write("\"fingerprint\":%s\n ,".format(JString(Base64.getUrlEncoder.withoutPadding.encodeToString(etag.asBytes))))
        writer.write("\"last_modified\":%s\n ,".format(JString(CJSONWriter.dateTimeFormat.print(lastModified))))
        writer.write("\"locale\":%s\n ,".format(JString(locale)))
        writer.write("\"schema\":")

        @AutomaticJsonEncode
        case class Field(c: ColumnName, t: SoQLType, h: Option[JValue])

        val reformattedSchema = nameAnalysis.statement.schema.values.map { case Statement.SchemaEntry(cn, typ, _hint, _isSynthetic) =>
          // not using the analysis's schema's hints because it's
          // post-rollup, so any hints inherited from parts of the
          // query served by a rollup will have been destroyed.
          Field(cn, typ, hintsByName.get(cn))
        }.toArray

        JsonUtil.writeJson(writer, reformattedSchema)
        writer.write("\n }")

        for(row <- rows) {
          writer.write("\n,")
          CompactJsonWriter.toWriter(writer, JArray(row))
        }

        writer.write("\n]\n")
      }
    }
  }

  def runQuery(
    conn: Connection,
    sql: String,
    systemContext: Option[Map[String, String]],
    cryptProviders: CryptProviderProvider,
    extractor: ResultExtractor[DatabaseNamesMetaTypes],
    rs: ResourceScope
  ): Iterator[Array[JValue]] = {
    systemContext.foreach(setupSystemContext(conn, _))

    new Iterator[Array[JValue]] {
      private val stmt = rs.open(conn.createStatement())
      stmt.setFetchSize(extractor.fetchSize)
      private val resultSet = rs.open(stmt.executeQuery(sql))
      private var done = false
      private var ready = false

      private val idReps = new scm.HashMap[Option[Provenance], JsonColumnWriteRep[SoQLType, SoQLValue]]
      private val versionReps = new scm.HashMap[Option[Provenance], JsonColumnWriteRep[SoQLType, SoQLValue]]

      private val types = extractor.schema.values.toArray
      private val width = types.length

      private def justNullRep(typ: SoQLType) =
        new JsonColumnWriteRep[SoQLType, SoQLValue] {
          override val representedType = typ
          override def toJValue(value: SoQLValue) = {
            assert(value == SoQLNull)
            JNull
          }
        }

      private def rep(typ: SoQLType, value: SoQLValue): JsonColumnWriteRep[SoQLType, SoQLValue] = {
        assert(value == SoQLNull || value.typ == typ)
        value match {
          case id: SoQLID =>
            idReps.get(id.provenance) match {
              case Some(rep) => rep
              case None =>
                val rep = new com.socrata.datacoordinator.common.soql.jsonreps.IDRep(new SoQLID.StringRep(id.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros)))
                idReps += id.provenance -> rep
                rep
            }
          case ver: SoQLVersion =>
            versionReps.get(ver.provenance) match {
              case Some(rep) => rep
              case None =>
                val rep = new com.socrata.datacoordinator.common.soql.jsonreps.VersionRep(new SoQLVersion.StringRep(ver.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros)))
                versionReps += ver.provenance -> rep
                rep
            }
          case SoQLNull if SoQLID == typ || SoQLVersion == typ => justNullRep(typ)
          case _notIdOrVersion => SoQLRep.jsonRepsMinusIdAndVersion(typ)
        }
      }

      override def hasNext: Boolean = {
        if(done) return false
        if(ready) return true

        ready = resultSet.next()
        done = !ready
        ready
      }

      def next(): Array[JValue] = {
        if(!hasNext) return Iterator.empty.next()
        ready = false

        val result = new Array[JValue](width)
        val row = extractor.extractRow(resultSet)

        var i = 0
        while(i != width) {
          result(i) = rep(types(i), row(i)).toJValue(row(i))
          i += 1
        }

        result
      }
    }
  }

  def setupSystemContext(conn: Connection, systemContext: Map[String, String]): Unit = {
    using(new ResourceScope) { rs =>
      val BatchSize = 5
      var fullStmt = Option.empty[PreparedStatement]

      def sql(n: Int) =
        Iterator.fill(n) { s"set_config('socrata_system.a' || md5(?), ?, true)" }.
          mkString("SELECT ", ", ", "")

      for(group <- systemContext.iterator.grouped(BatchSize)) {
        val stmt =
          group.length match {
            case BatchSize =>
              fullStmt match {
                case None =>
                  val stmt = rs.open(conn.prepareStatement(sql(BatchSize)))
                  fullStmt = Some(stmt)
                  stmt
                case Some(stmt) =>
                  stmt
              }
            case n =>
              rs.open(conn.prepareStatement(sql(n)))
          }

        for(((varName, varValue), i) <- group.iterator.zipWithIndex) {
          stmt.setString(1 + 2*i, varName)
          stmt.setString(2 + 2*i, varValue)
        }
        stmt.executeQuery().close()
      }
    }
  }

  def prettierSql[MT <: MetaTypes with SoQLMetaTypesExt](analysis: SoQLAnalysis[MT], doc: Doc[SqlizeAnnotation[MT]]): String = {
    val sb = new StringBuilder

    val map = new LabelMap(analysis)

    def walk(tree: SimpleDocTree[SqlizeAnnotation[MT]]): Unit = {
      tree match {
        case doctree.Empty =>
          // nothing
        case doctree.Char(c) =>
          sb.append(c)
        case doctree.Text(s) =>
          sb.append(s)
        case doctree.Line(n) =>
          sb.append('\n')
          var i = 0
          while(i < n) {
            sb.append(' ')
            i += 1
          }
        case doctree.Ann(a, doc) if doc.isInstanceOf[doctree.Ann[_]] =>
          walk(doc)
        case doctree.Ann(a, doc) =>
          a match {
            case SqlizeAnnotation.Expression(VirtualColumn(tLabel, cLabel, _)) =>
              walk(doc)
              for((rn, cn) <- map.virtualColumnNames.get((tLabel, cLabel))) {
                sb.append(" /* ")
                for(rn <- rn) {
                  sb.append(rn.name)
                  sb.append('.')
                }
                sb.append(cn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.Expression(PhysicalColumn(tLabel, _, cLabel, _)) =>
              walk(doc)
              for((rn, cn) <- map.physicalColumnNames.get((tLabel, cLabel))) {
                sb.append(" /* ")
                for(rn <- rn) {
                  sb.append(rn.name)
                  sb.append('.')
                }
                sb.append(cn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.Table(tLabel) =>
              walk(doc)
              for {
                rn <- map.tableNames.get(tLabel)
                rn <- rn
              } {
                sb.append(" /* ")
                sb.append(rn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.OutputName(name) =>
              walk(doc)
              sb.append(" /* ")
              sb.append(name.name)
              sb.append(" */")
            case _=>
              walk(doc)
          }
        case doctree.Concat(elems) =>
          elems.foreach(walk)
      }
    }
    walk(doc.layoutSmart().asTree)

    sb.toString
  }

  class LabelMap[MT <: MetaTypes](analysis: SoQLAnalysis[MT]) extends StatementUniverse[MT] {
    val tableNames = new scm.HashMap[AutoTableLabel, Option[ResourceName]]
    val virtualColumnNames = new scm.HashMap[(AutoTableLabel, ColumnLabel), (Option[ResourceName], ColumnName)]
    val physicalColumnNames = new scm.HashMap[(AutoTableLabel, DatabaseColumnName), (Option[ResourceName], ColumnName)]

    walkStmt(analysis.statement, None)

    private def walkStmt(stmt: Statement, currentLabel: Option[(AutoTableLabel, Option[ResourceName])]): Unit =
      stmt match {
        case CombinedTables(_, left, right) =>
          walkStmt(left, currentLabel)
          walkStmt(right, None)
        case CTE(defLbl, defAlias, defQ, _matHint, useQ) =>
          walkStmt(defQ, Some((defLbl, defAlias)))
          walkStmt(useQ, currentLabel)
        case v: Values =>
          for {
            (tl, rn) <- currentLabel
            (cl, schemaEntry) <- v.schema
          } {
            virtualColumnNames += (tl, cl) -> (rn, schemaEntry.name)
          }
        case sel: Select =>
          walkFrom(sel.from)
          for {
            (tl, rn) <- currentLabel
            (cl, ne) <- sel.selectList
          } {
            virtualColumnNames += (tl, cl) -> (rn, ne.name)
          }
      }

    private def walkFrom(from: From): Unit =
      from.reduce[Unit](
        walkAtomicFrom,
        { (_, join) => walkAtomicFrom(join.right) }
      )

    private def walkAtomicFrom(from: AtomicFrom): Unit =
      from match {
        case ft: FromTable =>
          tableNames += ft.label -> Some(ft.definiteResourceName.name)
          for((dcn, ne) <- ft.columns) {
            physicalColumnNames += (ft.label, dcn) -> (Some(ft.definiteResourceName.name), ne.name)
          }
        case fsr: FromSingleRow =>
          tableNames += fsr.label -> None
        case fs: FromStatement =>
          tableNames += fs.label -> fs.resourceName.map(_.name)
          walkStmt(fs.statement, Some((fs.label, fs.resourceName.map(_.name))))
      }
  }
}
