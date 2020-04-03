/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.util.Tabulator

trait AnalysisResult {
  def name: String
  def description: String = ""
}

trait TableResult extends AnalysisResult {
  def header: Seq[Any]
  def rows: Seq[RowResult]

  override def toString: String = {
    s"""
       |$name - $description
       |${Tabulator.format(header +: rows.map(_.toRow))}
       |""".stripMargin
  }
}

trait RowResult {
  def toRow: Seq[Any]
}

case class HeuristicResult(name: String,
                           results: Seq[AnalysisResult]) {
  override def toString: String = {
    s"""
      |$name
      |=============
      |${results.mkString("")}
      |""".stripMargin
  }
}

case class ConfigResult(rows: Seq[ConfigRowResult]) extends TableResult {
  override def name: String = "Config Suggestions"

  override def header: Seq[Any] = Seq("Key", "Value", "Suggested", "Description")
}

case class SimpleResult(name: String,
                        rows: Seq[SimpleRowResult],
                        header: Seq[Any] = Seq("Key", "Value")) extends TableResult {
}

case class ConfigRowResult(name: String,
                           value: String,
                           description: String,
                           suggestedValue: String = "") extends RowResult {
  override def toRow: Seq[Any] = Seq(name, value, suggestedValue, description)
}

case class SimpleRowResult(name: String,
                        description: String) extends RowResult {
  override def toRow: Seq[Any] = Seq(name, description)
}

