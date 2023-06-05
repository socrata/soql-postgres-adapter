package com.socrata.pg.analyzer2

import java.sql.ResultSet

import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._

class TestRepProvider(override val namespace: SqlNamespaces[SqlizerTest.TestMT]) extends Rep.Provider[SqlizerTest.TestMT] {
  type TestMT = SqlizerTest.TestMT

  override def mkStringLiteral(s: String) =
    Doc(JString(s).toString)

  def apply(typ: TestType): Rep = reps(typ)

  val reps = Map[TestType, Rep](
    TestID -> new ProvenancedRep(TestID, d"bigint") {
      def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[TestID]
        rawId.provenance.map(CanonicalName(_)).toSet
      }

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val rawId = e.value.asInstanceOf[TestID]

        val provLit = rawId.provenance match {
          case None => d"null :: text"
          case Some(s) => mkStringLiteral(s) +#+ d":: text"
        }
        val numLit = Doc(rawId.value) +#+ d":: bigint"

        ExprSql.Expanded[TestMT](Seq(provLit, numLit), e)
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }
    },
    TestText -> new SingleColumnRep(TestText, d"text") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val TestText(s) = e.value
        ExprSql(mkStringLiteral(s), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }
    },
    TestNumber -> new SingleColumnRep(TestNumber, d"numeric") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val TestNumber(n) = e.value
        ExprSql(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }
    },
    TestBoolean -> new SingleColumnRep(TestBoolean, d"boolean") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val TestBoolean(b) = e.value
        ExprSql(if(b) d"true" else d"false", e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }
    },

    TestCompound -> new CompoundColumnRep(TestCompound) {
      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql.Expanded[TestMT](Seq(d"null :: text", d"null :: numeric"), e)

      def expandedColumnCount = 2

      def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_a", base ++ d"_b")
      }

      def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val cmp@TestCompound(_, _) = e.value

        cmp match {
          case TestCompound(None, None) =>
            ExprSql.Expanded[TestMT](Seq(d"null :: text", d"null :: numeric"), e)
          case TestCompound(a, b) =>
            val aLit = a match {
              case Some(n) => mkStringLiteral(n)
              case None => d"null :: text"
            }
            val bLit = b match {
              case Some(n) => Doc(n.toString) +#+ d" :: numeric"
              case None => d"null :: numeric"
            }

            ExprSql.Expanded[TestMT](Seq(aLit, bLit), e)
        }
      }

      def subcolInfo(field: String) =
        field match {
          case "a" => SubcolInfo[TestMT](0, "text", TestText)
          case "b" => SubcolInfo[TestMT](1, "numeric", TestNumber)
        }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }
    }
  )
}