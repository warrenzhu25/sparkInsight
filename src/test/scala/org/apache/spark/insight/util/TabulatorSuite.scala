package org.apache.spark.insight.util

import org.scalatest.funsuite.AnyFunSuite

class TabulatorSuite extends AnyFunSuite {

  test("Tabulator.format should format a table correctly") {
    val table = Seq(Seq("a", "b"), Seq("1", "2"))
    val formattedTable = Tabulator.format(table)
    assert(formattedTable.nonEmpty)
  }
}
