/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.insight.util

object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row)
        yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  private def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
    rows.head ::
    rowSeparator ::
    rows.tail.toList :::
    rowSeparator ::
    List()).mkString("\n")

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes))
      yield if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", "|", "|")
  }

  private def rowSeparator(colSizes: Seq[Int]): String =
    colSizes map { "-" * _ } mkString("+", "+", "+")
}