
package org.apache.spark.insight.util

object Tabulator {
  def format(table: Seq[Seq[Any]]): String = {
    if (table.isEmpty) {
      return ""
    }

    val headers = table.head.map(_.toString).toArray
    val data = table.tail.map(_.map {
      case null => ""
      case other => other.toString
    }.toArray).toArray

    CliTable.create(headers, data)
  }
}
