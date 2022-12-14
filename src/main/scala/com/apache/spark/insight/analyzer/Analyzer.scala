package com.apache.spark.insight.analyzer

import com.apache.spark.insight.fetcher.SparkApplicationData
import com.apache.spark.insight.util.Tabulator

trait Analyzer {
  val name: String = this.getClass.getSimpleName

  def analysis(data: SparkApplicationData): AnalysisResult
}

case class AnalysisResult(
    name: String,
    header: Seq[String],
    rows: Seq[Seq[String]],
    description: String = "") {

  def toCliOutput: Unit = {
    print(s"""
             |$name - $description
             |${Tabulator.format(header +: rows)}
             |""".stripMargin)
  }
}
