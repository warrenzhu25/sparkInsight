package org.apache.spark.insight.analyzer

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.Tabulator
import org.apache.spark.util.Utils

/**
 * Trait for analyzing Spark application data.
 */
trait Analyzer {
  val name: String = this.getClass.getSimpleName

  def analysis(data: SparkApplicationData): AnalysisResult

  def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException(s"$name does not support diff analysis.")
  }
}

/**
 * Case class to hold the result of an analysis.
 *
 * @param name The name of the analysis.
 * @param header A sequence of header strings.
 * @param rows A sequence of rows, where each row is a sequence of strings.
 * @param description A description of the analysis.
 */
case class AnalysisResult(
    name: String,
    header: Seq[String],
    rows: Seq[Seq[String]],
    description: String = "") {

  def toCliOutput: Unit = {
    // scalastyle:off println
    println(s"""
             |$name - $description
             |${Tabulator.format(header +: rows)}
             |""".stripMargin)
    // scalastyle:on println
  }
}

object Analyzer{
  val metricInNano = Set("executorCpuTime", "shuffleWriteTime", "executorDeserializeCpuTime", "executorDeserializeTime")
}