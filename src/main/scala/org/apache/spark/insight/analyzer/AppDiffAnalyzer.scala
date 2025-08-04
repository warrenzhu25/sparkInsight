
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.StageData

import java.util.concurrent.TimeUnit

/**
 * An analyzer that compares two Spark applications.
 */
object AppDiffAnalyzer extends Analyzer {

  private val metrics = Seq(
    Metric("Disk Spill Size", s => s.diskBytesSpilled, "Total data spilled to disk (GB)", isSize = true),
    Metric("Executor CPU Time", s => s.executorCpuTime, "Total executor CPU time on main task thread (minutes)", isNanoTime = true),
    Metric("Executor Runtime", s => s.executorRunTime, "Total executor running time (minutes)", isTime = true),
    Metric("Input Records", s => s.inputRecords, "Total records consumed by tasks (thousands)", isRecords = true),
    Metric("Input Size", s => s.inputBytes, "Total input data consumed by tasks (GB)", isSize = true),
    Metric("JVM GC Time", s => s.jvmGcTime, "Total JVM garbage collection time (minutes)", isTime = true),
    Metric("Memory Spill Size", s => s.memoryBytesSpilled, "Total data spilled to memory (GB)", isSize = true),
    Metric("Output Records", s => s.outputRecords, "Total records produced by tasks (thousands)", isRecords = true),
    Metric("Output Size", s => s.outputBytes, "Total output data produced by tasks (GB)", isSize = true),
    Metric("Shuffle Read Records", s => s.shuffleReadRecords, "Total shuffle records consumed by tasks (thousands)", isRecords = true),
    Metric("Shuffle Read Size", s => s.shuffleReadBytes, "Total shuffle data consumed by tasks (GB)", isSize = true),
    Metric("Shuffle Read Wait Time", s => s.shuffleFetchWaitTime, "Total task time blocked waiting for remote shuffle data (minutes)", isTime = true),
    Metric("Shuffle Write Records", s => s.shuffleWriteRecords, "Total shuffle records produced by tasks (thousands)", isRecords = true),
    Metric("Shuffle Write Size", s => s.shuffleWriteBytes, "Total shuffle data produced by tasks (GB)", isSize = true),
    Metric("Shuffle Write Time", s => s.shuffleWriteTime, "Total shuffle write time spent by tasks (minutes)", isTime = true)
  )

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val appEndTime1 = data1.appInfo.attempts.head.endTime.getTime
    val totalExecutorTime1 = data1.executorSummaries.map { exec =>
      val removeTime = exec.removeTime.map(_.getTime).getOrElse(appEndTime1)
      removeTime - exec.addTime.getTime
    }.sum
    val totalRuntime1 = data1.appInfo.attempts.head.duration

    val appEndTime2 = data2.appInfo.attempts.head.endTime.getTime
    val totalExecutorTime2 = data2.executorSummaries.map { exec =>
      val removeTime = exec.removeTime.map(_.getTime).getOrElse(appEndTime2)
      removeTime - exec.addTime.getTime
    }.sum
    val totalRuntime2 = data2.appInfo.attempts.head.duration

    val rows = metrics.flatMap { metric =>
      val value1 = data1.stageData.map(metric.value).sum(Numeric.LongIsIntegral)
      val value2 = data2.stageData.map(metric.value).sum(Numeric.LongIsIntegral)
      if (value1 == 0 && value2 == 0) {
        None
      } else {
        val diff = value2 - value1
        val diffPercentage = if (value1 == 0) "N/A" else f"${(diff * 100.0 / value1)}%.2f%%"

        Some(Seq(
          metric.name,
          FormatUtils.formatValue(value1, metric.isTime, metric.isNanoTime, metric.isSize, metric.isRecords),
          FormatUtils.formatValue(value2, metric.isTime, metric.isNanoTime, metric.isSize, metric.isRecords),
          s"$diffPercentage",
          metric.description
        ))
      }
    }

    val totalExecutorTimeDiff = totalExecutorTime2 - totalExecutorTime1
    val totalExecutorTimeDiffPercentage = if (totalExecutorTime1 == 0) "N/A" else f"${(totalExecutorTimeDiff * 100.0 / totalExecutorTime1)}%.2f%%"
    val totalRuntimeDiff = totalRuntime2 - totalRuntime1
    val totalRuntimeDiffPercentage = if (totalRuntime1 == 0) "N/A" else f"${(totalRuntimeDiff * 100.0 / totalRuntime1)}%.2f%%"

    val derivedRows = Seq(
      Seq(
        "Total Executor Time",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalExecutorTime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalExecutorTime2)}",
        s"$totalExecutorTimeDiffPercentage",
        "Total time across all executors (minutes)"
      ),
      Seq(
        "Total Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime2)}",
        s"$totalRuntimeDiffPercentage",
        "Total elapsed running time (minutes)"
      )
    )

    val headers = Seq("Metric", "App1", "App2", "Diff", "Metric Description")
    AnalysisResult(
      s"Spark Application Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      headers,
      rows ++ derivedRows)
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    // This analyzer requires two applications, so this method is not supported.
    throw new UnsupportedOperationException("AppDiffAnalyzer requires two Spark applications to compare.")
  }
}
