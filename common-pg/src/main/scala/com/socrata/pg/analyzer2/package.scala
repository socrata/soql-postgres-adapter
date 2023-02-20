package com.socrata.pg

import com.socrata.prettyprint.prelude._

package object analyzer2 {
  implicit class AugmentSeq[T](private val underlying: Seq[Doc[T]]) extends AnyVal {
    def commaSep: Doc[T] =
      underlying.concatWith { (l, r) =>
        l ++ Doc.Symbols.comma ++ Doc.lineSep ++ r
      }

    def funcall[U >: T](functionName: Doc[U]): Doc[U] =
      ((functionName ++ d"(" ++ Doc.lineCat ++ commaSep).nest(2) ++ Doc.lineCat ++ d")").group

    def parenthesized: Doc[T] =
      ((d"(" ++ Doc.lineCat ++ commaSep).nest(2) ++ Doc.lineCat ++ d")").group
  }

  implicit class AugmentDoc[T](private val underlying: Doc[T]) extends AnyVal {
    def funcall[U >: T](functionName: Doc[U]): Doc[U] =
      Seq(underlying).funcall(functionName)

    def parenthesized: Doc[T] =
      Seq(underlying).parenthesized
  }

  implicit class LayoutSingleLine[T](private val doc: Doc[T]) extends AnyVal {
    def layoutSingleLine = {
      // println(doc)
      doc.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    }
  }
}
