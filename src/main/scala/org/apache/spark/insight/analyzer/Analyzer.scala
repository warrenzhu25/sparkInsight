package org.apache.spark.insight.analyzer

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.Tabulator
import org.apache.spark.util.Utils

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

case class Metric(
    name: String,
    value: Long,
) {
  private def displayText(): String = {
    try {
      if (Analyzer.metricInNano.contains(name)) {
        s"${Duration(value, TimeUnit.NANOSECONDS).toMinutes} mins"
      } else if (name.contains("Time")) {
        s"${Duration(value, TimeUnit.MILLISECONDS).toMinutes} mins"
      } else if (name.contains("Bytes") || name.contains("Memory") || name.contains("Size")) {
        Utils.bytesToString(value)
      } else if (name.contains("Records")) {
        f"${value}%,d"
      } else {
        value.toString
      }
    } catch {
      case e :Exception => println(s"Failed to format $name $value")
        value.toString
    }
  }

  def toRow(): Seq[String] = {
    Seq(name, displayText())
  }
}

object Analyzer{
  val metricInNano = Set("executorCpuTime", "shuffleWriteTime", "executorDeserializeCpuTime", "executorDeserializeTime")
}
