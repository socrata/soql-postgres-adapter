package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, PivotQuery, SoQLAnalysis, SubAnalysis}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.typed._
import com.socrata.soql.types._

import scala.util.parsing.input.NoPosition

object BinarySoQLAnalysisSqlizer extends Sqlizer[(BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], Map[TableName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])]
  with SoQLAnalysisSqlizer
{
  import Sqlizer._
  import SqlizerContext._

  def sql(analysisTablesReps: AnalysisTarget)
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val (analysis, tableNames, allColumnReps) = analysisTablesReps
    val primaryTable = tableNames(TableName.PrimaryTable)
    val analysisWithFrom = updateFrom(analysis, TableName(primaryTable))
    val tableNamesWithPrimaryTable = tableNames + (TableName(primaryTable) -> primaryTable)
    val ctxWithOutermostSoqls =
      if (ctx.contains(SqlizerContext.OutermostSoqls)) ctx
      else ctx + (SqlizerContext.OutermostSoqls -> BinarySoQLAnalysisSqlizer.outerMostAnalyses(analysisWithFrom))
    val (psql, _) = sql(analysisWithFrom, tableNamesWithPrimaryTable, allColumnReps, reqRowCount = false, rep, typeRep, setParams, ctxWithOutermostSoqls, escape, None)
    psql
  }


  /**
   * For rowcount w/o group by, just replace the select with count(*).
   * For rowcount with group by, wrap the original group by sql with a select count(*) from ( {original}) t_rc
   */
  def rowCountSql(analysisTablesReps: AnalysisTarget)
                 (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  ctx: Context,
                  escape: Escape): ParametricSql = {
    val (analysis, tableNames, allColumnReps) = analysisTablesReps
    val primaryTable = tableNames(TableName.PrimaryTable)
    val analysisWithFrom = updateFrom(analysis, TableName(primaryTable))
    val rcAnalysis = rowCountAnalysis(analysisWithFrom)
    val keepRequestRowCount = rcAnalysis.eq(analysisWithFrom)
    val tableNamesWithPrimaryTable = tableNames + (TableName(primaryTable) -> primaryTable)
    val ctxWithOutermostSoqls =
      if (ctx.contains(SqlizerContext.OutermostSoqls)) ctx
      else ctx + (SqlizerContext.OutermostSoqls -> BinarySoQLAnalysisSqlizer.outerMostAnalyses(rcAnalysis))
    val (psql, _) = sql(rcAnalysis, tableNamesWithPrimaryTable, allColumnReps, reqRowCount = keepRequestRowCount, rep, typeRep, setParams, ctxWithOutermostSoqls, escape, None)
    psql
  }

  private def rowCountAnalysis(analysis: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {
    analysis match {
      case PipeQuery(_, _) =>
        analysis
      case Compound(_, _, _) =>
        val selection = OrderedMap(com.socrata.soql.environment.ColumnName("count") -> (FunctionCall(SoQLFunctions.CountStar.monomorphic.get, Seq.empty, None)(NoPosition, NoPosition)))
        val countAnalysis = SoQLAnalysis[UserColumnId, SoQLType](false, false, selection, None, Seq.empty, None, Seq.empty, None, Seq.empty, None, None, None)
        PipeQuery(analysis, Leaf(countAnalysis))
      case _ =>
        analysis
    }
  }

  private def updateFrom(analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], tableName: TableName): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {
    analyses match {
      case Compound(op, left, right) =>
        Compound(op, updateFrom(left, tableName), right)
      case leaf@Leaf(analysis) =>
        if (analysis.from.isEmpty) Leaf(analysis.copy(from = Some(tableName)))
        else leaf
    }
  }

  private def sql(analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], // scalastyle:ignore method.length parameter.number
                  tableNames: Map[TableName, String],
                  allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
                  reqRowCount: Boolean,
                  rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  ctx: Context,
                  escape: Escape,
                  fromTableName: Option[TableName] = None): (ParametricSql, /* params count in select, excluding where, group by... */ Int) = {

    analyses match {
      case PipeQuery(l, Leaf(ra)) =>
        val (lpsql, _) = sql(l, tableNames, allColumnReps, reqRowCount, rep, typeRep, setParams, ctx, escape, fromTableName)
        val (primaryTableName, chainedTableAlias) = ra.from match {
            case Some(tn@TableName(name, Some(alias))) if name == TableName.This =>
              (tn.copy(alias = None), alias)
            case _ =>
              (TableName.PrimaryTable, "x1")
          }
        val subTableName = if (ra.from.isEmpty) "(%s) AS %s".format(lpsql.sql.head, chainedTableAlias)
                           else "(%s)".format(lpsql.sql.head)
        val tableNamesSubTableNameReplace = tableNames + (primaryTableName -> subTableName)
        val primaryTableAlias = if (ra.joins.nonEmpty) Map((PrimaryTableAlias -> chainedTableAlias)) else Map.empty // REVISIT
        val subCtx = ctx ++ primaryTableAlias
        val prevAna = l.outputSchemaLeaf
        val (rpsql, rParamsCountInSelect) =
          toSql(ra, Option(prevAna), tableNamesSubTableNameReplace, allColumnReps, reqRowCount, rep, typeRep,
                setParams, subCtx, escape, fromTableName)
        // query parameters in the select phrase come before those in the sub-query.
        // query parameters in the other phrases - where, group by, order by, etc come after those in the sub-query.
        val (setParamsBefore, setParamsAfter) = rpsql.setParams.splitAt(rParamsCountInSelect)
        val setParamsAcc = setParamsBefore ++ lpsql.setParams ++ setParamsAfter
        (rpsql.copy(setParams = setParamsAcc), rParamsCountInSelect)
      case PipeQuery(_, _) =>
        throw new Exception("right operand of pipe query cannot be recursive")
      case PivotQuery(l, r) =>
        println("xxxx")
        val ctxPivot = ctx + (LeftOfPivot -> true)
        val (lpsql, lpcts) = sql(l, tableNames, allColumnReps, reqRowCount, rep, typeRep, setParams, ctxPivot, escape, fromTableName)
        val rleaf = r.outputSchema.leaf
        val pivotSql = toPivotSql(rleaf, lpsql, tableNames, allColumnReps, reqRowCount, rep, typeRep,
          setParams, ctx, escape, rleaf.from.orElse(fromTableName))
        println(pivotSql.sql.mkString(""))
        (pivotSql, lpcts)
      case Compound(op, l, r) =>
        val (lpsql, lpcts) = sql(l, tableNames, allColumnReps, reqRowCount, rep, typeRep, setParams, ctx, escape, fromTableName)
        val (rpsql, rpcts) = sql(r, tableNames, allColumnReps, reqRowCount, rep, typeRep, setParams, ctx, escape, fromTableName)
        val setParamsAcc = lpsql.setParams ++ rpsql.setParams
        val sqlQueryOp = toSqlQueryOp(op)
        val unionSql = lpsql.sql.zip(rpsql.sql).map { case (ls, rs) =>
          if (r.asLeaf.nonEmpty) s"${ls} $sqlQueryOp ${rs}"
          else s"${ls} $sqlQueryOp (${rs})"
        }
        (ParametricSql(unionSql, setParamsAcc), lpcts + rpcts)
      case Leaf(analysis) =>
        toSql(analysis, None, tableNames, allColumnReps, reqRowCount, rep, typeRep,
              setParams, ctx, escape, analysis.from.orElse(fromTableName))
    }
  }

  def toPivotSql(
              analysis: SoQLAnalysis[UserColumnId, SoQLType], // scalastyle:ignore method.length parameter.number
              lpsql: ParametricSql,
              tableNames: Map[TableName, String],
              allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
              reqRowCount: Boolean,
              rep0: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
              typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
              setParams: Seq[SetParam],
              context: Context,
              escape: Escape,
              fromTableName: Option[TableName] = None): ParametricSql = {

    // select * from crosstab('sql') as ct(name text,  biology decimal, chemistry decimal, physics decimal)
    val p1 =  lpsql.sql.mkString("")
    val p1b = StringLiteralSqlizer.quote(p1, escape)
    println(p1)
    println(p1b)
    val cols = analysis.selection.values.toList.map {
      case FunctionCall(function, Seq(StringLiteral(name, _)), _) =>
        val st = function.result
        s"${name} ${toDbType(st)}"
      case ColumnRef(_, column, st) =>
        s"${column.underlying} ${toDbType(st)}"
    }
    val p2 = cols.mkString("as tt(", ",", ")")

    val sql = s"SELECT * FROM crosstab(${p1b}) ${p2}"
    lpsql.copy(sql = Seq(sql))
  }


  private def toDbType(st: SoQLType): String = {
    st match {
      case SoQLNumber => "decimal"
      case _ => "text"
    }
  }

  private def toSqlQueryOp(soqlQueryOp: String): String = {
    soqlQueryOp match {
      case "MINUS" => "EXCEPT"
      case op => op
    }
  }

  def outerMostAnalyses(analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], set: Set[SoQLAnalysis[UserColumnId, SoQLType]] = Set.empty):
    Set[SoQLAnalysis[UserColumnId, SoQLType]] = {
    analyses match {
      case PipeQuery(_, r) =>
        outerMostAnalyses(r, set)
      case Compound(_, l, r) =>
        outerMostAnalyses(l, set) ++ outerMostAnalyses(r, set)
      case Leaf(analysis) =>
        set + analysis
    }
  }
}

trait SoQLAnalysisSqlizer {
  this: Sqlizer[(BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], Map[TableName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])] =>

  import Sqlizer._
  import SqlizerContext._

  /**
   * Convert one analysis to parameteric sql.
   */
  def toSql(analysis: SoQLAnalysis[UserColumnId, SoQLType], // scalastyle:ignore method.length parameter.number
            prevAna: Option[SoQLAnalysis[UserColumnId, SoQLType]],
            tableNames: Map[TableName, String],
            allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
            reqRowCount: Boolean,
            rep0: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
            typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
            setParams: Seq[SetParam],
            context: Context,
            escape: Escape,
            fromTableName: Option[TableName] = None): (ParametricSql, /* params count in select, excluding where, group by... */ Int) = {

    // Use leading search despite being poor in semantics.  It is more likely to use the GIN index and performs better.
    // TODO: switch to trailing search when there is smarter index support
    val leadingSearch = context.getOrElse(LeadingSearch, true) != false
    val tableNamesWithThis =
      if (tableNames.contains(TableName(TableName.This, None))) tableNames
      else tableNames + (TableName(TableName.This, None) -> tableNames(TableName.PrimaryTable))

    val rep = analysis.from match {
      case Some(tn@TableName(name, Some(_))) if name == TableName.This =>
        rep0.flatMap {
          case (QualifiedUserColumnId(None, r), x) =>
            Map((QualifiedUserColumnId(None, r) -> x)) ++
              Map((QualifiedUserColumnId(Some(TableName.This), r) -> x)) ++
              Map((QualifiedUserColumnId(Some(tn.aliasWithoutPrefix.get), r) -> x))
          case (x, y) =>
            Map(x -> y)
        }
      case _ =>
        rep0
    }

    val isOutermost = isOutermostAnalysis(analysis, context)
    val reqRowCountFinal = reqRowCount && isOutermost
    val ana = if (reqRowCountFinal) rowCountAnalysis(analysis) else analysis

    val joins = analysis.joins

    val fromTableNames = ana.from.foldLeft(Map.empty[TableName, String]) { (acc, from) =>
      acc + (from.copy(alias = None) -> from.name)
    }
    val fromTableAliases = ana.from.foldLeft(Map.empty[String, String]) { (acc, from) =>
      from.alias match {
        case Some(alias) =>
          acc + (alias -> from.name)
        case None =>
          acc
      }
    }

    val parentSimpleJoinMap = context.get(SimpleJoinMap) match {
      case Some(map) => map.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val simpleJoinMapAcc: Map[String, String] = parentSimpleJoinMap ++ (analysis.from match {
      case Some(TableName(TableName.This, Some(alias))) =>
        if (prevAna.isDefined) {
          Map.empty
        } else {
          val name = tableNames(TableName.PrimaryTable)
          Map(alias -> name, name -> alias)
        }
      case Some(TableName(name, Some(alias))) =>
        Map(alias -> name, name -> alias)
      case Some(TableName(name, None)) =>
        Map(name -> name)
      case _ =>
        Map.empty
    })

    val simpleJoinMap = joins.foldLeft(simpleJoinMapAcc) { (acc, j) =>
      j.from.subAnalysis match {
        case Left(TableName(name, Some(alias))) =>
          acc + (alias -> name) + (name -> alias)
        case Left(TableName(name, None)) =>
          // simple join w/o alias does not need remap
          acc + (name -> name)
        case _ =>
          // complex join reps are just qualifier.name and use type reps and don't need entries in reps.  They use
          acc
      }
    }

    val prevTableAliasMap = context.get(TableAliasMap) match {
      case Some(x) => x.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val tableAliasMap = (prevTableAliasMap ++ tableNames.map {
        case (k, v) =>
          (k.qualifier, realAlias(k, v))
      } ++ fromTableAliases)

    val ctx = context + (Analysis -> analysis) +
      (SimpleJoinMap -> simpleJoinMap) +
      (InnermostSoql -> analysis.from.isDefined) +
      (OutermostSoql -> isOutermost) +
      (TableMap -> (tableNames ++ fromTableNames)) +
      (TableAliasMap -> tableAliasMap)

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)
    val joinTableNames = joins.foldLeft(Map.empty[TableName, String]) { (acc, j) =>
      acc ++ j.from.alias.map(x => TableName(x, None) -> x)
    }

    val joinTableAliases = joins.foldLeft(Map.empty[String, String]) { (acc, j) =>
      j.from.subAnalysis match {
        case Left(TableName(name, None)) =>
          acc
        case Left(TableName(name, alias)) =>
          acc + (alias.getOrElse(name) -> name)
        case Right(SubAnalysis(analyses, alias)) =>
          acc + (alias -> alias)
      }
    }

    val repByQualifier = rep.groupBy(_._1.qualifier)
    val repFrom = analysis.from.foldLeft(Map.empty[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) { (acc, tableName) =>
      tableName match {
        case TableName(name, a@Some(alias)) =>
          val joinRep = repByQualifier(Some(name))
          val newJoinRep = joinRep.map { case (QualifiedUserColumnId(qualifier, userColumnId), sqlColumnRep) =>
            (QualifiedUserColumnId(a, userColumnId), sqlColumnRep)
          }
          acc ++ newJoinRep
        case TableName(name, None) =>
          repByQualifier.get(Some(name)) match {
            case Some(joinRep) =>
              val newJoinRep = joinRep.map { case (QualifiedUserColumnId(qualifier, userColumnId), sqlColumnRep) =>
                (QualifiedUserColumnId(None, userColumnId), sqlColumnRep)
              }
              acc ++ newJoinRep
            case None =>
              acc
          }
      }
    }

    val repJoins = joins.foldLeft(repFrom) { (acc, join) =>
      join.from.subAnalysis match {
        case Left(TableName(name, a@Some(alias))) =>
          val joinRep = repByQualifier(Some(name))
          val newJoinRep = joinRep.map { case (QualifiedUserColumnId(qualifier, userColumnId), sqlColumnRep) =>
            (QualifiedUserColumnId(a, userColumnId), sqlColumnRep)
          }
          acc ++ newJoinRep
        case _ =>
          acc
      }
    }

    val tableAlias = ctx(TableAliasMap).asInstanceOf[Map[String, String]]
    val defaultTableName = analysis.from.foldLeft(Map.empty[TableName, String]) { (acc, from) =>
      acc + (TableName.PrimaryTable -> from.qualifier)
    }
    val defaultTableAlias = analysis.from.foldLeft(Map.empty[String, String]) { (acc, from) =>
      val qualifier = from.qualifier
      acc + (TableName.PrimaryTable.qualifier -> tableAlias(qualifier))
    }

    val tableNamesWithJoins = ctx(TableMap).asInstanceOf[Map[TableName, String]] ++ joinTableNames ++ defaultTableName
    val tableAliasesWithJoins = ctx(TableAliasMap).asInstanceOf[Map[String, String]] ++ joinTableAliases ++ defaultTableAlias
    val ctxSelectWithJoins = ctxSelect + (TableMap -> tableNamesWithJoins) + (TableAliasMap -> tableAliasesWithJoins)

    // TODO: rename repMinusComplexJoinTable. name retained for continuity
    val repMinusComplexJoinTable = rep ++ repJoins

    val (selectPhrase, setParamsSelect) =
      if (reqRowCountFinal && analysis.groupBys.isEmpty && analysis.search.isEmpty) {
        (List("count(*)"), setParams)
      } else {
        select(analysis)(repMinusComplexJoinTable, typeRep, setParams, ctxSelectWithJoins, escape)
      }

    val paramsCountInSelect = setParamsSelect.size - setParams.size

    // JOIN
    val (joinPhrase, setParamsJoin) = joins.foldLeft((Seq.empty[String], Seq.empty[SetParam])) { case ((sqls, setParamAcc), join) =>
      val fromTableName = join.from.subAnalysis match {
        case Right(SubAnalysis(analyses, alias)) =>
          analyses.seq.head.from.map(_.name).get
        case Left(tableName) => tableName.name
      }

      val joinTableName =  TableName(fromTableName, None)
      val joinTableNames = tableNames + (TableName.PrimaryTable -> tableNames(joinTableName))

      val ctxJoin = ctx +
        (SoqlPart -> SoqlJoin) +
        (TableMap -> joinTableNames) +
        (IsSubQuery -> true)

      val (tableName, joinOnParams) = join.from.subAnalysis match {
        case Right(SubAnalysis(analyses, alias)) =>
          val repJoin = if (join.lateral) repFrom ++ rep else rep
          val joinTableLikeParamSql = Sqlizer.sql(
            (analyses, joinTableNames, allColumnReps))(repJoin, typeRep, Seq.empty, ctxJoin, escape)
          val tn = "(" + joinTableLikeParamSql.sql.mkString + ") as " + alias
          (tn, setParamAcc ++ joinTableLikeParamSql.setParams)
        case Left(tableName) =>
          val key = TableName(tableName.name, None)
          val realTableName = tableNames(key)
          val qualifier = tableName.qualifier
          val tn = if (tableName.alias.isEmpty) realTableName else s"$realTableName as $qualifier"
          (tn, setParamAcc)
      }

      val repJoin = if (join.lateral) repFrom ++ repMinusComplexJoinTable
                    else repMinusComplexJoinTable
      val joinConditionParamSql = Sqlizer.sql(join.on)(repJoin, typeRep, joinOnParams, ctxSelectWithJoins + (SoqlPart -> SoqlJoin), escape)
      val joinCondition = joinConditionParamSql.sql.mkString(" ")
      val lateral = if (join.lateral) "LATERAL " else ""
      (sqls :+ s" ${join.typ.toString} ${lateral}$tableName ON $joinCondition", joinConditionParamSql.setParams)
    }

    val setParamsSelectJoin = setParamsSelect ++ setParamsJoin
    // WHERE
    val where = ana.where.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsSelectJoin, ctxSelectWithJoins + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelectJoin)

    // SEARCH in WHERE
    val ParametricSql(whereSearch, setParamsWhereSearch) = ana.search match {
      case Some(s) if (leadingSearch || !ana.isGrouped) =>
        val searchAna = if (leadingSearch) leadingSearchAnalysis(ana, prevAna, rep) else ana
        sqlizeSearch(s, leadingSearch, searchAna)(repMinusComplexJoinTable, typeRep, setParamsWhere, ctxSelectWithJoins + (SoqlPart -> SoqlSearch), escape)
      case _ =>
        ParametricSql(Seq.empty[String], setParamsWhere)
    }

    // GROUP BY
    val isGroupByAllConstants = !ana.groupBys.exists(!Sqlizer.isLiteral(_))
    val groupBy = ana.groupBys.foldLeft((List.empty[String], setParamsWhereSearch)) { (t2, gb) =>
      val ParametricSql(sqls, newSetParams) =
        if (Sqlizer.isLiteral(gb)) {
          if (isGroupByAllConstants) {
            // SELECT 'literal' as alias GROUP BY 'literal' does not work in SQL
            // But that's how the analysis come back from GROUP BY alias
            // Use group by position.
            val groupByColumnPosition = analysis.selection.values.toSeq.indexWhere(_ == gb) + 1
            ParametricSql(Seq(groupByColumnPosition.toString), t2._2)
          } else {
            // SoQL does not support group by column position.  Group by literal does not do anything.
            ParametricSql(Seq.empty, t2._2)
          }
        } else {
          Sqlizer.sql(gb)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlGroup), escape)
        }
      (t2._1 ++ sqls, newSetParams)
    }
    val setParamsGroupBy = groupBy._2

    // HAVING
    val having = ana.having.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsGroupBy, ctxSelectWithJoins + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // SEARCH in HAVING
    val ParametricSql(havingSearch, setParamsHavingSearch) = ana.search match {
      case Some(s) if (!leadingSearch && ana.isGrouped) =>
        sqlizeSearch(s, leadingSearch, ana)(repMinusComplexJoinTable, typeRep, setParamsHaving, ctxSelectWithJoins + (SoqlPart -> SoqlSearch), escape)
      case _ =>
        ParametricSql(Seq.empty[String], setParamsHaving)
    }

    // ORDER BY
    val orderBy = ana.orderBys.foldLeft((Seq.empty[String], setParamsHavingSearch)) { (t2, ob) =>
      val ParametricSql(sqls, newSetParams) =
        Sqlizer.sql(ob)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
      (t2._1 ++ sqls, newSetParams)
    }
    val setParamsOrderBy = orderBy._2

    val tableName = analysis.from.orElse(fromTableName).getOrElse(TableName.PrimaryTable)
    val from =
      if (prevAna.isEmpty && fromTableName.isEmpty) ""
      else s" FROM ${tableNamesWithThis(tableName.copy(alias = None))}" + tableName.alias.map(a => s" as ${a}").getOrElse("")

    // COMPLETE SQL
    val selectOptionalDistinct = "SELECT " + (if (analysis.distinct) "DISTINCT " else "")
    val completeSql = selectPhrase.mkString(selectOptionalDistinct, ",", "") +
      from +
      joinPhrase.mkString(" ") +
      where.flatMap(_.sql.headOption.map(" WHERE " +  _)).getOrElse("") +
      whereSearch.mkString(" ") +
      (if (ana.groupBys.nonEmpty) groupBy._1.mkString(" GROUP BY ", ",", "") else "") +
      having.flatMap(_.sql.headOption.map(" HAVING " + _)).getOrElse("") +
      havingSearch.mkString(" ") +
      (if (ana.orderBys.nonEmpty) orderBy._1.mkString(" ORDER BY ", ",", "") else "") +
      ana.limit.map(" LIMIT " + _.toString).getOrElse("") +
      ana.offset.map(" OFFSET " + _.toString).getOrElse("")

    val result = ParametricSql(Seq(countBySubQuery(analysis)(reqRowCountFinal, completeSql)), setParamsOrderBy)
    (result, paramsCountInSelect)
  }

  private def countBySubQuery(analysis: SoQLAnalysis[UserColumnId, SoQLType])(reqRowCount: Boolean, sql: String) =
    if (reqRowCount && (analysis.groupBys.nonEmpty || analysis.search.nonEmpty)) s"SELECT count(*) FROM ($sql) t_rc" else sql


  private def leadingSearchAnalysis(analysis: SoQLAnalysis[UserColumnId, SoQLType], // scalastyle:ignore method.length parameter.number
                                    prevAna: Option[SoQLAnalysis[UserColumnId, SoQLType]],
                                    rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]]): SoQLAnalysis[UserColumnId, SoQLType] = {
    prevAna match {
      case Some(ana) =>
        val sel = ana.selection.map { case (cn: ColumnName, expr: CoreExpr[UserColumnId, SoQLType]) =>
          val userColumnId = new UserColumnId(cn.caseFolded)
          val cr = new ColumnRef(None, userColumnId, expr.typ)(expr.position)
          (cn, cr)
        }
        // where and having are used for determining how to combine the search condition.  Only its existance matters.  The actual content doesn't.
        ana.copy(selection = sel, where = analysis.where, having = analysis.having)
      case None =>
        var i = 0
        val allDefaultColumns = rep.collect {
          case (QualifiedUserColumnId(None, userColumnId), sqlColumnRep: SqlColumnRep[SoQLType, SoQLValue]) =>
            val cn = com.socrata.soql.environment.ColumnName(userColumnId.underlying)
            val cr = new ColumnRef(None, userColumnId, sqlColumnRep.representedType)(NoPosition)
            i += 1
            (cn, (i, cr))
        }.toMap
        val map = new com.socrata.soql.collection.OrderedMap(allDefaultColumns, allDefaultColumns.keys.toVector)
        analysis.copy(selection = map)
    }
  }

  def sqlizeSearch(searchStr: String, leadingSearch: Boolean, ana: SoQLAnalysis[UserColumnId, SoQLType])
                  (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Context,
                   escape: Escape): ParametricSql = {

    val searchLit = StringLiteral[SoQLType](searchStr, SoQLText)(NoPosition)

    val (searchText, setParamsSearchText) = searchVectorText(ana)(rep, typeRep, setParams, ctx + (SoqlPart -> SoqlSearch), escape)

    val ParametricSql(Seq(searchSql), setParamsSearchLit) =
      Sqlizer.sql(searchLit)(rep, typeRep, setParamsSearchText, ctx + (SoqlPart -> SoqlSearch), escape)

    val searchNumberLitOpt =
      try {
        val number = BigDecimal.apply(searchLit.value)
        Some(NumberLiteral(number, SoQLNumber.t)(searchLit.position))
      } catch {
        case _: NumberFormatException =>
          None
      }

    val (searchVectorSql, searchVectorSetParams) = searchText match {
      case None =>
        (Seq("false"), setParamsSearchText)
      case Some(sv) =>
        (Seq(s"$sv @@ plainto_tsquery('english', $searchSql)"), setParamsSearchLit)
    }

    val (searchNumber, setParamsSearchNumber) = searchNumberLitOpt match {
      case None => (Seq.empty[String], searchVectorSetParams)
      case Some(searchNumberLit) =>
        searchVectorNumber(ana)(rep, typeRep, searchVectorSetParams, ctx + (SoqlPart -> SoqlSearch), escape)
    }

    val searchTextPlusNumberParametricSql = searchNumber.foldLeft(ParametricSql(searchVectorSql, setParamsSearchNumber)) { (acc, x) =>
      val ParametricSql(Seq(prev), setParamsBefore) = acc
      val ParametricSql(Seq(searchSql), setParamsAfter) =
        Sqlizer.sql(searchNumberLitOpt.get)(rep, typeRep, setParamsBefore, ctx + (SoqlPart -> SoqlSearch), escape)
      ParametricSql(Seq(s"$prev OR ($x = $searchSql)"), setParamsAfter)
    }

    val andOrWhereOrHaving = ana.isGrouped && !leadingSearch match {
      case false => if (ana.where.isDefined) " AND" else " WHERE"
      case true => if (ana.having.isDefined) " AND" else " HAVING"
    }

    if (searchTextPlusNumberParametricSql.sql.isEmpty) {
      ParametricSql(Seq(s"$andOrWhereOrHaving false"), setParams)
    } else {
      ParametricSql(searchTextPlusNumberParametricSql.sql.map { x => s"$andOrWhereOrHaving ($x)" }, searchTextPlusNumberParametricSql.setParams)
    }
  }

  private def searchVectorText(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                              (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                               typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                               setParams: Seq[SetParam],
                               ctx: Context,
                               escape: Escape) = {

    val (sqls, setParamsSearch) =
      analysis.selection.view.filter(x => PostgresUniverseCommon.SearchableTypes.contains(x._2.typ))
                             .foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, columnNameAndcoreExpr) =>
        val (columnName, coreExpr) = columnNameAndcoreExpr
        val (_, setParamsNew) = acc
        val ctxSelect = ctx + (RootExpr -> coreExpr)
        val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, setParamsNew, ctxSelect, escape)
        (acc._1 ++ sqls, newSetParams)
      }
    (PostgresUniverseCommon.toTsVector(sqls.zip(sqls)), setParamsSearch)
  }

  private def searchVectorNumber(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                                (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                 typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                 setParams: Seq[SetParam],
                                 ctx: Context,
                                 escape: Escape) = {

    analysis.selection.view.filter(x => PostgresUniverseCommon.SearchableNumericTypes.contains(x._2.typ))
                           .foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val ctxSelect = ctx + (RootExpr -> coreExpr)
      val (_, setParamsNew) = acc
      val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, setParamsNew, ctxSelect, escape)
      (acc._1 :+ sqls.head, newSetParams) // each sql-string is a column
    }
  }

  /**
   * This cannot handle SoQLLocation because it is mapped to multiple sql columns.
   * SoQLLocation is handled in Sqlizer.
   */
  private val GeoTypes: Set[SoQLType] =
    Set(SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine, SoQLPolygon, SoQLMultiPolygon, SoQLLocation)

  /**
   * When we pull data out of pg we only want to translate it when we pull it out for performance reasons,
   * in particular if we are doing aggregations on geo types in the SQL query, so we do so against the top
   * level types of the final select list.
   */
  private def toGeoText(sql: String, typ: SoQLType, columnName: Option[ColumnName]): String = {
    if (GeoTypes.contains(typ)) { s"ST_AsBinary($sql)" + columnName.map(x => s" AS ${x.name}").getOrElse("") }
    else { sql }
  }

  private def select(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                    (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                     typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                     setParams: Seq[SetParam],
                     ctx: Context,
                     escape: Escape) = {
    val strictOutermostSoql = isStrictOutermostSoql(ctx)
    val (sqls, setParamsInSelect) =
      // Chain select handles setParams differently than others.  The current setParams are prepended
      // to existing setParams.  Others are appended.
      // That is why setParams starts with empty in foldLeft.
      analysis.selection.foldLeft(Tuple2(Seq.empty[String], Seq.empty[SetParam])) { (acc, columnNameAndcoreExpr) =>
        val (columnName, coreExpr) = columnNameAndcoreExpr
        val ctxSelect = ctx + (RootExpr -> coreExpr) + (SqlizerContext.ColumnName -> columnName.name)
        val (_, selectSetParams) = acc
        val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, selectSetParams, ctxSelect, escape)
        val sqlGeomConverted =
          if (shouldConvertGeomToText(ctx) && !sqls.isEmpty) {
            val cn = if (strictOutermostSoql) Some(columnName) else None
            // compound type with a geometry and something else like "Location" type
            // must place the geometry in the first part.
            sqls.updated(0, toGeoText(sqls.head, coreExpr.typ, cn))
          } else {
            sqls
          }
        (acc._1 ++ sqlGeomConverted, newSetParams)
      }
    (sqls, setParamsInSelect ++ setParams)
  }

  private def shouldConvertGeomToText(ctx: Context): Boolean = {
    !(ctx.contains(LeaveGeomAsIs) || ctx.contains(IsSubQuery) || false == ctx(OutermostSoql))
  }

  /**
   * Basically, analysis for row count has select (except when there is search), limit and offset removed.
   * TODO: select count(*) // w/o group by which result is always 1.
   * @param a original query analysis
   * @return Analysis for generating row count sql.
   */
  private def rowCountAnalysis(a: SoQLAnalysis[UserColumnId, SoQLType]) = {
    a.copy(selection = if (a.search.isEmpty) a.selection.empty else a.selection,
           orderBys = Nil,
           limit = None,
           offset = None)
  }

  private def isOutermostAnalysis(analysis: SoQLAnalysis[_, _], ctx: Context) = {
    ctx.get(OutermostSoqls) match {
      case Some(set) =>
        set.asInstanceOf[Set[SoQLAnalysis[_, _]]].contains(analysis)
      case None =>
        ctx(OutermostSoql) == true
    }
  }

  private def isStrictOutermostSoql(ctx: Context): Boolean = {
    ctx(OutermostSoql) == true && ctx(InnermostSoql) == false
  }
}
