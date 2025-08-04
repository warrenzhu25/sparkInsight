package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.StageData

import java.util.concurrent.TimeUnit

/**
 * An analyzer that compares two Spark applications.
 */
object AppDiffAnalyzer extends Analyzer {

  case class Metric(
      name: String,
      value: StageData => Long,
      isTime: Boolean = false,
      isNanoTime: Boolean = false,
      isSize: Boolean = false,
      isRecords: Boolean = false)

  private val metrics = Seq(
    Metric("Disk Spill Size", s => s.diskBytesSpilled, isSize = true),
    Metric("Executor CPU Time", s => s.executorCpuTime, isNanoTime = true),
    Metric("Executor Runtime", s => s.executorRunTime, isTime = true),
    Metric("Input Records", s => s.inputRecords, isRecords = true),
    Metric("Input Size", s => s.inputBytes, isSize = true),
    Metric("JVM GC Time", s => s.jvmGcTime, isTime = true),
    Metric("Memory Spill Size", s => s.memoryBytesSpilled, isSize = true),
    Metric("Output Records", s => s.outputRecords, isRecords = true),
    Metric("Output Size", s => s.outputBytes, isSize = true),
    Metric("Shuffle Read Records", s => s.shuffleReadRecords, isRecords = true),
    Metric("Shuffle Read Size", s => s.shuffleReadBytes, isSize = true),
    Metric("Shuffle Read Wait Time", s => s.shuffleFetchWaitTime, isTime = true),
    Metric("Shuffle Write Records", s => s.shuffleWriteRecords, isRecords = true),
    Metric("Shuffle Write Size", s => s.shuffleWriteBytes, isSize = true),
    Metric("Shuffle Write Time", s => s.shuffleWriteTime, isTime = true)
  )

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val rows = metrics.map { metric =>
      val value1 = data1.stageData.map(metric.value).sum(Numeric.LongIsIntegral)
      val value2 = data2.stageData.map(metric.value).sum(Numeric.LongIsIntegral)
      val diff = value2 - value1
      val diffPercentage = if (value1 == 0) "N/A" else f"${(diff * 100.0 / value1)}%.2f%%"

      Seq(
        metric.name,
        FormatUtils.formatValue(value1, metric.isTime, metric.isNanoTime, metric.isSize, metric.isRecords),
        FormatUtils.formatValue(value2, metric.isTime, metric.isNanoTime, metric.isSize, metric.isRecords),
        s"${FormatUtils.formatValue(diff, metric.isTime, metric.isNanoTime, metric.isSize, metric.isRecords)} ($diffPercentage)"
      )
    }

    val headers = Seq("Metric", "App1", "App2", "Diff")
    AnalysisResult(
      s"Spark Application Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      headers,
      rows)
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    // This analyzer requires two applications, so this method is not supported.
    throw new UnsupportedOperationException("AppDiffAnalyzer requires two Spark applications to compare.")
  }
}