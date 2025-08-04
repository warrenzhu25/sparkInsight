package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.TaskData

import java.util.concurrent.TimeUnit

object ShuffleSkewAnalyzer extends Analyzer {

  private val SHUFFLE_READ_THRESHOLD = 10 * 1024 * 1024 * 1024L // 10GB

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val skewedStages = data.stageData.filter(_.shuffleReadBytes > SHUFFLE_READ_THRESHOLD).flatMap { stage =>
      val stageId = s"${stage.stageId}.${stage.attemptId}"
      val tasks = data.taskData.getOrElse(stageId, Seq.empty)
      if (tasks.nonEmpty) {
        val executorShuffleReads = tasks.groupBy(_.executorId).mapValues(_.map(_.taskMetrics.get.shuffleReadMetrics.recordsRead).sum)
        val avgShuffleRead = executorShuffleReads.values.sum.toDouble / executorShuffleReads.size
        val skewedExecutors = executorShuffleReads.filter(_._2.toDouble > avgShuffleRead * 2)
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
      executors.map { case (executorId, shuffleRead) =>
        Seq(
          stageId,
          executorId,
          FormatUtils.formatValue(shuffleRead, isTime = false, isNanoTime = false, isSize = false, isRecords = true)
        )
      }
    }

    val headers = Seq("Stage ID", "Executor ID", "Shuffle Read Records")
    AnalysisResult(
      s"Shuffle Skew Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Shows executors with shuffle read significantly larger than the average for stages with shuffle read > 10GB."
    )
  }

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ShuffleSkewAnalyzer only supports single application analysis.")
  }
}
