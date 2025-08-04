
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.StageData

import java.util.concurrent.TimeUnit

/**
 * An analyzer that generates a summary of Spark application.
 */
object AppSummaryAnalyzer extends Analyzer {

  case class Metric(
      name: String,
      value: StageData => Long,
      description: String,
      isTime: Boolean = false,
      isNanoTime: Boolean = false,
      isSize: Boolean = false,
      isRecords: Boolean = false)

  private val metrics = Seq(
    Metric(
      "Disk Spill Size",
      s => s.diskBytesSpilled,
      "Total data spilled to disk (GB)",
      isSize = true),
    Metric(
      "Executor CPU Time",
      s => s.executorCpuTime,
      "Total executor CPU time on main task thread (minutes)",
      isNanoTime = true),
    Metric(
      "Executor Runtime",
      s => s.executorRunTime,
      "Total executor running time (minutes)",
      isTime = true),
    Metric(
      "Input Records",
      s => s.inputRecords,
      "Total records consumed by tasks (thousands)",
      isRecords = true),
    Metric(
      "Input Size",
      s => s.inputBytes,
      "Total input data consumed by tasks (GB)",
      isSize = true),
    Metric(
      "JVM GC Time",
      s => s.jvmGcTime,
      "Total JVM garbage collection time (minutes)",
      isTime = true),
    Metric(
      "Memory Spill Size",
      s => s.memoryBytesSpilled,
      "Total data spilled to memory (GB)",
      isSize = true),
    Metric(
      "Output Records",
      s => s.outputRecords,
      "Total records produced by tasks (thousands)",
      isRecords = true),
    Metric(
      "Output Size",
      s => s.outputBytes,
      "Total output data produced by tasks (GB)",
      isSize = true),
    Metric(
      "Shuffle Read Records",
      s => s.shuffleReadRecords,
      "Total shuffle records consumed by tasks (thousands)",
      isRecords = true),
    Metric(
      "Shuffle Read Size",
      s => s.shuffleReadBytes,
      "Total shuffle data consumed by tasks (GB)",
      isSize = true),
    Metric(
      "Shuffle Read Wait Time",
      s => s.shuffleFetchWaitTime,
      "Total task time blocked waiting for remote shuffle data (minutes)",
      isTime = true),
    Metric(
      "Shuffle Write Records",
      s => s.shuffleWriteRecords,
      "Total shuffle records produced by tasks (thousands)",
      isRecords = true),
    Metric(
      "Shuffle Write Size",
      s => s.shuffleWriteBytes,
      "Total shuffle data produced by tasks (GB)",
      isSize = true),
    Metric(
      "Shuffle Write Time",
      s => s.shuffleWriteTime,
      "Total shuffle write time spent by tasks (minutes)",
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
      val formattedValue = FormatUtils.formatValue(totalValue, m.isTime, m.isNanoTime, m.isSize, m.isRecords)
      Seq(m.name, formattedValue, m.description)
    }

    val derivedMetrics = Seq(
      Seq(
        "Total Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime)}",
        "Total elapsed running time (minutes)"),
      Seq(
        "Executor Runtime w/o Shuffle",
        s"${TimeUnit.MILLISECONDS.toMinutes(executorRuntimeWithoutShuffle)}",
        "Executor run time excluding shuffle time (minutes)"),
      Seq(
        "Net I/O Time",
        s"${netIOTime / (1024 * 1024 * 1024)}",
        "Total time accessing external storage (minutes)")
    )

    val allMetrics = (calculatedMetrics ++ derivedMetrics).sortBy(_.head)

    val headers = Seq("Metric name", "Value", "Metric description")
    val rows = allMetrics

    AnalysisResult(s"Spark Application Performance Report for applicationId: ${sparkAppData.appInfo.id}", headers, rows)
  }
}
