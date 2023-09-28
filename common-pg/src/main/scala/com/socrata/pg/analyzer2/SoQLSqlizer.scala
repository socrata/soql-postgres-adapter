package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types.obfuscation.CryptProvider

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

class SoQLSqlizer(
  escapeString: String => String,
  cryptProviderProvider: CryptProviderProvider,
  override val systemContext: Map[String, String],
  locationSubcolumns: SoQLSqlizer.LocationSubcolumns
) extends Sqlizer[DatabaseNamesMetaTypes] {
  override val namespace = new SqlNamespaces {
    def databaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(dataTableName) = dtn
      Doc(dataTableName.name)
    }
    def databaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(physicalColumnBase) = dcn
      Doc(physicalColumnBase)
    }
  }

  override val toProvenance = DatabaseNamesMetaTypes.provenanceMapper
  override def isRollup(dtn: DatabaseTableName) = dtn.name.isRollup

  override def mkRepProvider(physicalTableFor: Map[AutoTableLabel, DatabaseTableName]): Rep.Provider[DatabaseNamesMetaTypes] =
    new SoQLRepProvider[DatabaseNamesMetaTypes](
      cryptProviderProvider,
      namespace,
      toProvenance,
      isRollup,
      locationSubcolumns,
      physicalTableFor
    ) {
      override def mkStringLiteral(text: String): Doc = {
        // By default, converting a String to Doc replaces the newlines
        // with soft newlines which can be converted into spaces by
        // `group`.  This is a thing we _definitely_ don't want, so
        // instead replace those newlines with hard line breaks, and
        // un-nest lines by the current nesting level so the linebreak
        // doesn't introduce any indentation.
        val escapedText = escapeString(text)
          .split("\n", -1)
          .toSeq
          .map(Doc(_))
          .concatWith { (a: Doc, b: Doc) =>
            a ++ Doc.hardline ++ b
          }
        val unindented = Doc.nesting { depth => escapedText.nest(-depth) }
        d"'" ++ unindented ++ d"'"
      }
    }

  override val funcallSqlizer = SoQLSqlizer.funcallSqlizer

  override val rewriteSearch = SoQLSqlizer.rewriteSearch
}

object SoQLSqlizer extends SqlizerUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]
  private val funcallSqlizer = new SoQLFunctionSqlizer[DatabaseNamesMetaTypes]
  private val rewriteSearch = new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true)
}
