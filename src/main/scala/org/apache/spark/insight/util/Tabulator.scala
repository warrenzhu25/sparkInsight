package org.apache.spark.insight.util

/**
 * Utility for formatting data into a text-based table.
 */
object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val stringTable = table.map(_.map(_.toString))
      val colSizes = stringTable.transpose.map(_.map(_.length).max)
      val rows = for (row <- stringTable) yield formatRow(row, colSizes)
      val separator = rowSeparator(colSizes)
      formatRows(separator, rows)
  }

  private def formatRows(rowSeparator: String, rows: Seq[String]): String = (rowSeparator ::
    rows.head ::
    rowSeparator ::
    rows.tail.toList :::
    rowSeparator ::
    List()).mkString("\n")

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells =
      for ((item, size) <- row.zip(colSizes))
        yield if (size == 0) "" else (" %-" + size + "s ").format(item)
    cells.mkString("| ", " | ", " |")
  }

  private def rowSeparator(colSizes: Seq[Int]): String =
    colSizes.map {
      col =>
        "-" * (col + 3)
    }.mkString("+", "+", "+")
}