
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.{StageData, StageStatus}

import java.util.concurrent.TimeUnit

/**
 * An analyzer that compares two Spark applications.
 */
object AppDiffAnalyzer extends Analyzer {

  private val metrics = Seq(
    // Time
    Metric("Executor CPU Time", s => s.executorCpuTime, "Total executor CPU time on main task thread (minutes)", isNanoTime = true),
    Metric("Executor Runtime", s => s.executorRunTime, "Total executor running time (minutes)", isTime = true),
    Metric("JVM GC Time", s => s.jvmGcTime, "Total JVM garbage collection time (minutes)", isTime = true),
    Metric("Shuffle Read Wait Time", s => s.shuffleFetchWaitTime, "Total task time blocked waiting for remote shuffle data (minutes)", isTime = true),
    Metric("Shuffle Write Time", s => s.shuffleWriteTime, "Total shuffle write time spent by tasks (minutes)", isTime = true),
    // Size
    Metric("Disk Spill Size", s => s.diskBytesSpilled, "Total data spilled to disk (GB)", isSize = true),
    Metric("Memory Spill Size", s => s.memoryBytesSpilled, "Total data spilled to memory (GB)", isSize = true),
    Metric("Input Size", s => s.inputBytes, "Total input data consumed by tasks (GB)", isSize = true),
    Metric("Output Size", s => s.outputBytes, "Total output data produced by tasks (GB)", isSize = true),
    Metric("Shuffle Read Size", s => s.shuffleReadBytes, "Total shuffle data consumed by tasks (GB)", isSize = true),
    Metric("Shuffle Write Size", s => s.shuffleWriteBytes, "Total shuffle data produced by tasks (GB)", isSize = true),
    // Records
    Metric("Input Records", s => s.inputRecords, "Total records consumed by tasks (thousands)", isRecords = true),
    Metric("Output Records", s => s.outputRecords, "Total records produced by tasks (thousands)", isRecords = true),
    Metric("Shuffle Read Records", s => s.shuffleReadRecords, "Total shuffle records consumed by tasks (thousands)", isRecords = true),
    Metric("Shuffle Write Records", s => s.shuffleWriteRecords, "Total shuffle records produced by tasks (thousands)", isRecords = true)
  )

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val appEndTime1 = data1.appInfo.attempts.head.endTime.getTime
    val totalExecutorTime1 = data1.executorSummaries.map { exec =>
      val removeTime = exec.removeTime.map(_.getTime).getOrElse(appEndTime1)
      removeTime - exec.addTime.getTime
    }.sum
    val totalRuntime1 = data1.appInfo.attempts.head.duration
    val (successfulStages1, failedStages1) = data1.stageData.partition(_.status == StageStatus.COMPLETE)
    val successfulExecutorRuntime1 = successfulStages1.map(_.executorRunTime).sum
    val failedExecutorRuntime1 = failedStages1.map(_.executorRunTime).sum
    val executorCores1 = data1.appConf.getOrElse("spark.executor.cores", "1").toInt
    val executorUtilization1 = if (totalExecutorTime1 == 0) 0.0 else (data1.stageData.map(_.executorRunTime).sum.toDouble / (totalExecutorTime1 * executorCores1)) * 100

    val appEndTime2 = data2.appInfo.attempts.head.endTime.getTime
    val totalExecutorTime2 = data2.executorSummaries.map { exec =>
      val removeTime = exec.removeTime.map(_.getTime).getOrElse(appEndTime2)
      removeTime - exec.addTime.getTime
    }.sum
    val totalRuntime2 = data2.appInfo.attempts.head.duration
    val (successfulStages2, failedStages2) = data2.stageData.partition(_.status == StageStatus.COMPLETE)
    val successfulExecutorRuntime2 = successfulStages2.map(_.executorRunTime).sum
    val failedExecutorRuntime2 = failedStages2.map(_.executorRunTime).sum
    val executorCores2 = data2.appConf.getOrElse("spark.executor.cores", "1").toInt
    val executorUtilization2 = if (totalExecutorTime2 == 0) 0.0 else (data2.stageData.map(_.executorRunTime).sum.toDouble / (totalExecutorTime2 * executorCores2)) * 100

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
    val successfulExecutorRuntimeDiff = successfulExecutorRuntime2 - successfulExecutorRuntime1
    val successfulExecutorRuntimeDiffPercentage = if (successfulExecutorRuntime1 == 0) "N/A" else f"${(successfulExecutorRuntimeDiff * 100.0 / successfulExecutorRuntime1)}%.2f%%"
    val failedExecutorRuntimeDiff = failedExecutorRuntime2 - failedExecutorRuntime1
    val failedExecutorRuntimeDiffPercentage = if (failedExecutorRuntime1 == 0) "N/A" else f"${(failedExecutorRuntimeDiff * 100.0 / failedExecutorRuntime1)}%.2f%%"
    val executorUtilizationDiff = executorUtilization2 - executorUtilization1
    val executorUtilizationDiffPercentage = if (executorUtilization1 == 0) "N/A" else f"${(executorUtilizationDiff * 100.0 / executorUtilization1)}%.2f%%"

    val timeRows = Seq(
      Seq(
        "Total Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime2)}",
        s"$totalRuntimeDiffPercentage",
        "Total elapsed running time (minutes)"
      ),
      Seq(
        "Total Executor Time",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalExecutorTime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(totalExecutorTime2)}",
        s"$totalExecutorTimeDiffPercentage",
        "Total time across all executors (minutes)"
      ),
      Seq(
        "Successful Executor Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(successfulExecutorRuntime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(successfulExecutorRuntime2)}",
        s"$successfulExecutorRuntimeDiffPercentage",
        "Total executor running time for successful stages (minutes)"
      ),
      Seq(
        "Failed Executor Runtime",
        s"${TimeUnit.MILLISECONDS.toMinutes(failedExecutorRuntime1)}",
        s"${TimeUnit.MILLISECONDS.toMinutes(failedExecutorRuntime2)}",
        s"$failedExecutorRuntimeDiffPercentage",
        "Total executor running time for failed stages (minutes)"
      ),
      Seq(
        "Executor Utilization",
        f"$executorUtilization1%.2f%%",
        f"$executorUtilization2%.2f%%",
        s"$executorUtilizationDiffPercentage",
        "Executor utilization percentage"
      )
    ) ++ rows.filter(r => r.head.contains("Time"))

    val headers = Seq("Metric", "App1", "App2", "Diff", "Metric Description")
    val categoryHeader = Seq("", "", "", "", "")
    val allMetrics = Seq(
      Seq("Time", "", "", "", ""),
      categoryHeader
    ) ++ timeRows ++ Seq(
      Seq("I/O", "", "", "", ""),
      categoryHeader
    ) ++ rows.filter(r => r.head.contains("Input") || r.head.contains("Output")) ++ Seq(
      Seq("Shuffle", "", "", "", ""),
      categoryHeader
    ) ++ rows.filter(r => r.head.contains("Shuffle") || r.head.contains("Spill"))

    AnalysisResult(
      s"Spark Application Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      headers,
      allMetrics)
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    // This analyzer requires two applications, so this method is not supported.
    throw new UnsupportedOperationException("AppDiffAnalyzer requires two Spark applications to compare.")
  }
}
