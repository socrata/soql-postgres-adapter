package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.pg.analyzer2.SqlizerUniverse

class SoQLSplitAnd[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends SplitAnd[MT]
{
  private val And = SoQLFunctions.And.monomorphic.get

  override def split(e: Expr): NonEmptySeq[Expr] =
    e match {
      case FunctionCall(And, args) =>
        val nonEmptyArgs = NonEmptySeq.fromSeq(args).getOrElse {
          throw new Exception("AND with no arguments??")
        }
        nonEmptyArgs.flatMap(split)
      case other =>
        NonEmptySeq(other)
    }

  override def merge(es: NonEmptySeq[Expr]): Expr =
    es.foldLeft1(identity) { (acc, expr) => FunctionCall[MT](And, Seq(acc, expr))(FuncallPositionInfo.None) }
}
