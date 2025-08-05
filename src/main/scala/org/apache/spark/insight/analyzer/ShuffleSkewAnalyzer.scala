package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils
import org.apache.spark.status.api.v1.TaskData

import java.util.concurrent.TimeUnit

object ShuffleSkewAnalyzer extends Analyzer {

  private val SHUFFLE_WRITE_THRESHOLD = 1 * 1024 * 1024 * 1024L // 1GB

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val skewedStages = data.stageData.filter(_.shuffleWriteBytes > SHUFFLE_WRITE_THRESHOLD).flatMap { stage =>
      val stageId = s"${stage.stageId}.${stage.attemptId}"
      val tasks = data.taskData.getOrElse(stageId, Seq.empty)
      if (tasks.nonEmpty) {
        val executorShuffleWrites = tasks.groupBy(_.executorId).mapValues(_.map(_.taskMetrics.get.shuffleWriteMetrics.recordsWritten).sum)
        val avgShuffleWrite = executorShuffleWrites.values.sum.toDouble / executorShuffleWrites.size
        val skewedExecutors = executorShuffleWrites.filter(_._2.toDouble > avgShuffleWrite * 2)
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
      executors.map { case (executorId, shuffleWrite) =>
        Seq(
          stageId,
          executorId,
          FormatUtils.formatValue(shuffleWrite, isTime = false, isNanoTime = false, isSize = false, isRecords = true)
        )
      }
    }

    val headers = Seq("Stage ID", "Executor ID", "Shuffle Write Records")
    AnalysisResult(
      s"Shuffle Skew Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Shows executors with shuffle write significantly larger than the average for stages with shuffle write > 1GB."
    )
  }

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ShuffleSkewAnalyzer only supports single application analysis.")
  }
}
