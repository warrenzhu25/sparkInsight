
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.StageData

import java.util.concurrent.TimeUnit

/**
 * An analyzer that generates a summary of the Spark application.
 */
object AppSummaryAnalyzer extends Analyzer {

  case class Metric(
      name: String,
      value: StageData => Long,
      description: String,
      isTime: Boolean = false,
      isSize: Boolean = false,
      isRecords: Boolean = false)

  private val metrics = Seq(
    Metric(
      "Disk Spill Size",
      s => s.diskBytesSpilled,
      "Total data spilled to disk (in GB)",
      isSize = true),
    Metric(
      "Executor CPU Time",
      s => s.executorCpuTime,
      "Total active CPU time spent by the executor running the main task thread (in minutes)",
      isTime = true),
    Metric(
      "Executor Runtime",
      s => s.executorRunTime,
      "Total elapsed time spent by the executor running tasks (in minutes)",
      isTime = true),
    Metric(
      "Input Records",
      s => s.inputRecords,
      "Total number of records consumed by tasks (in thousands)",
      isRecords = true),
    Metric(
      "Input Size",
      s => s.inputBytes,
      "Total input data consumed by tasks (in GB)",
      isSize = true),
    Metric(
      "JVM GC Time",
      s => s.jvmGcTime,
      "Total time JVM spent in garbage collection (in minutes)",
      isTime = true),
    Metric(
      "Memory Spill Size",
      s => s.memoryBytesSpilled,
      "Total data spilled to memory (in GB)",
      isSize = true),
    Metric(
      "Output Records",
      s => s.outputRecords,
      "Total number of records produced by tasks (in thousands)",
      isRecords = true),
    Metric(
      "Output Size",
      s => s.outputBytes,
      "Total output data produced by tasks (in GB)",
      isSize = true),
    Metric(
      "Shuffle Read Records",
      s => s.shuffleReadRecords,
      "Total number of shuffle records consumed by tasks (in thousands)",
      isRecords = true),
    Metric(
      "Shuffle Read Size",
      s => s.shuffleReadBytes,
      "Total shuffle data consumed by tasks (in GB)",
      isSize = true),
    Metric(
      "Shuffle Read Wait Time",
      s => s.shuffleFetchWaitTime,
      "Total time during which tasks were blocked waiting for remote shuffle data (in minutes)",
      isTime = true),
    Metric(
      "Shuffle Write Records",
      s => s.shuffleWriteRecords,
      "Total number of shuffle records produced by tasks (in thousands)",
      isRecords = true),
    Metric(
      "Shuffle Write Size",
      s => s.shuffleWriteBytes,
      "Total shuffle data produced by tasks (in GB)",
      isSize = true),
    Metric(
      "Shuffle Write Time",
      s => s.shuffleWriteTime,
      "Total Shuffle write time spent by tasks (in minutes)",
      isTime = true)
  )

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData
    val totalRuntime = sparkAppData.appInfo.attempts.head.duration
    val totalShuffleTime = stageData.map(_.shuffleFetchWaitTime).sum
    val executorRuntimeWithoutShuffle = stageData.map(_.executorRunTime).sum - totalShuffleTime
    val netIOTime = stageData.map(s => s.inputBytes + s.outputBytes).sum(Numeric.LongIsIntegral)

    val calculatedMetrics = metrics.map { m =>
      val totalValue = stageData.map(m.value).sum(Numeric.LongIsIntegral)
      val formattedValue = formatValue(totalValue, m.isTime, m.isSize, m.isRecords)
      Seq(m.name, formattedValue, m.description)
    }

    val derivedMetrics = Seq(
      Seq(
        "Total Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime)}",
        "Total elapsed running time (in minutes)"),
      Seq(
        "Executor Runtime w/o Shuffle",
        s"${TimeUnit.MILLISECONDS.toMinutes(executorRuntimeWithoutShuffle)}",
        "Executor run time excluding shuffle time (in minutes)"),
      Seq(
        "Net I/O Time",
        s"${netIOTime / (1024 * 1024 * 1024)}",
        "Total time spent accessing external storage (in minutes)")
    )

    val allMetrics = (calculatedMetrics ++ derivedMetrics).sortBy(_.head)

    val headers = Seq("Metric name", "Value", "Metric description")
    val rows = allMetrics

    AnalysisResult(s"Spark Application Performance Report for applicationId: ${sparkAppData.appInfo.id}", headers, rows)
  }

  private def formatValue(value: Long, isTime: Boolean, isSize: Boolean, isRecords: Boolean): String = {
    if (isTime) {
      s"${TimeUnit.MILLISECONDS.toMinutes(value)}"
    } else if (isSize) {
      s"${value / (1024 * 1024 * 1024)}"
    } else if (isRecords) {
      s"${value / 1000}"
    } else {
      value.toString
    }
  }
}
