package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.{StageData, StageStatus}

import java.util.concurrent.TimeUnit

/**
 * An analyzer that generates a summary of Spark application.
 */
object AppSummaryAnalyzer extends Analyzer {

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData
    val appInfo = sparkAppData.appInfo
    val appEndTime = appInfo.attempts.head.endTime.getTime
    val totalRuntime = appInfo.attempts.head.duration
    val totalShuffleTime = stageData.map(_.shuffleFetchWaitTime).sum
    val executorRuntimeWithoutShuffle = stageData.map(_.executorRunTime).sum - totalShuffleTime
    val netIOTime = stageData.map(s => s.inputBytes + s.outputBytes).sum(Numeric.LongIsIntegral)
    val totalExecutorTime = sparkAppData.executorSummaries.map { exec =>
      val removeTime = exec.removeTime.map(_.getTime).getOrElse(appEndTime)
      removeTime - exec.addTime.getTime
    }.sum

    val (successfulStages, failedStages) = stageData.partition(_.status == StageStatus.COMPLETE)
    val successfulExecutorRuntime = successfulStages.map(_.executorRunTime).sum
    val failedExecutorRuntime = failedStages.map(_.executorRunTime).sum

    def getMetricValue(value: StageData => Long, isTime: Boolean = false, isNanoTime: Boolean = false, isSize: Boolean = false, isRecords: Boolean = false): String = {
      val totalValue = stageData.map(value).sum(Numeric.LongIsIntegral)
      FormatUtils.formatValue(totalValue, isTime, isNanoTime, isSize, isRecords)
    }

    val durationMetrics = Seq(
      Seq("Total Runtime", s"${TimeUnit.MILLISECONDS.toMinutes(totalRuntime)}", "Total elapsed running time (minutes)"),
      Seq("Total Executor Time", s"${TimeUnit.MILLISECONDS.toMinutes(totalExecutorTime)}", "Total time across all executors (minutes)"),
      Seq("Successful Executor Runtime", s"${TimeUnit.MILLISECONDS.toMinutes(successfulExecutorRuntime)}", "Total executor running time for successful stages (minutes)"),
      Seq("Failed Executor Runtime", s"${TimeUnit.MILLISECONDS.toMinutes(failedExecutorRuntime)}", "Total executor running time for failed stages (minutes)"),
      Seq("Executor Runtime w/o Shuffle", s"${TimeUnit.MILLISECONDS.toMinutes(executorRuntimeWithoutShuffle)}", "Executor run time excluding shuffle time (minutes)")
    )

    val executorMetrics = Seq(
      Seq("Executor CPU Time", getMetricValue(_.executorCpuTime, isNanoTime = true), "Total executor CPU time on main task thread (minutes)"),
      Seq("JVM GC Time", getMetricValue(_.jvmGcTime, isTime = true), "Total JVM garbage collection time (minutes)")
    )

    val ioMetrics = Seq(
      Seq("Input Size", getMetricValue(_.inputBytes, isSize = true), "Total input data consumed by tasks (GB)"),
      Seq("Output Size", getMetricValue(_.outputBytes, isSize = true), "Total output data produced by tasks (GB)"),
      Seq("Input Records", getMetricValue(_.inputRecords, isRecords = true), "Total records consumed by tasks (thousands)"),
      Seq("Output Records", getMetricValue(_.outputRecords, isRecords = true), "Total records produced by tasks (thousands)"),
      Seq("Net I/O Time", s"${netIOTime / (1024 * 1024 * 1024)}", "Total time accessing external storage (minutes)")
    )

    val shuffleMetrics = Seq(
      Seq("Shuffle Read Size", getMetricValue(_.shuffleReadBytes, isSize = true), "Total shuffle data consumed by tasks (GB)"),
      Seq("Shuffle Write Size", getMetricValue(_.shuffleWriteBytes, isSize = true), "Total shuffle data produced by tasks (GB)"),
      Seq("Shuffle Read Records", getMetricValue(_.shuffleReadRecords, isRecords = true), "Total shuffle records consumed by tasks (thousands)"),
      Seq("Shuffle Write Records", getMetricValue(_.shuffleWriteRecords, isRecords = true), "Total shuffle records produced by tasks (thousands)"),
      Seq("Shuffle Read Wait Time", getMetricValue(_.shuffleFetchWaitTime, isTime = true), "Total task time blocked waiting for remote shuffle data (minutes)"),
      Seq("Shuffle Write Time", getMetricValue(_.shuffleWriteTime, isTime = true), "Total shuffle write time spent by tasks (minutes)"),
      Seq("Disk Spill Size", getMetricValue(_.diskBytesSpilled, isSize = true), "Total data spilled to disk (GB)"),
      Seq("Memory Spill Size", getMetricValue(_.memoryBytesSpilled, isSize = true), "Total data spilled to memory (GB)")
    )

    val headers = Seq("Metric name", "Value", "Metric description")
    val categoryHeader = Seq("", "", "")
    val allMetrics = Seq(
      Seq("Duration", "", ""),
      categoryHeader
    ) ++ durationMetrics ++ Seq(
      Seq("Executor", "", ""),
      categoryHeader
    ) ++ executorMetrics ++ Seq(
      Seq("I/O", "", ""),
      categoryHeader
    ) ++ ioMetrics ++ Seq(
      Seq("Shuffle", "", ""),
      categoryHeader
    ) ++ shuffleMetrics

    AnalysisResult(s"Spark Application Performance Report for applicationId: ${sparkAppData.appInfo.id}", headers, allMetrics)
  }
}