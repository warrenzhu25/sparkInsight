package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.TaskData

import java.util.concurrent.TimeUnit

object ShuffleSkewAnalyzer extends Analyzer {

  private val SHUFFLE_WRITE_THRESHOLD = 100 * 1024 * 1024L // 100MB

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val skewedStages = data.stageData.filter(_.shuffleWriteBytes > SHUFFLE_WRITE_THRESHOLD).flatMap { stage =>
      val stageId = s"${stage.stageId}.${stage.attemptId}"
      val executorSummary = stage.executorSummary.get
      if (executorSummary.nonEmpty) {
        val avgShuffleWrite = executorSummary.values.map(_.shuffleWrite).sum.toDouble / executorSummary.size
        val skewedExecutors = executorSummary.filter(_._2.shuffleWrite > avgShuffleWrite * 2)
        if (skewedExecutors.nonEmpty) {
          Some(stageId -> skewedExecutors)
        } else {
          None
        }
      } else {
        None
      }
    }

    val rows = skewedStages.flatMap { case (stageId, executors) =>
      executors.map { case (executorId, summary) =>
        Seq(
          stageId,
          executorId,
          FormatUtils.formatValue(summary.shuffleWrite, isTime = false, isNanoTime = false, isSize = true, isRecords = false)
        )
      }
    }

    val headers = Seq("Stage ID", "Executor ID", "Shuffle Write Size (GB)")
    AnalysisResult(
      s"Shuffle Skew Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Shows executors with shuffle write significantly larger than the average for stages with shuffle write > 100MB."
    )
  }

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ShuffleSkewAnalyzer only supports single application analysis.")
  }
}
