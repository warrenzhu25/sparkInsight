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
      val distributions = stage.executorMetricsDistributions
      if (distributions.isDefined) {
        val shuffleWrite = distributions.get.shuffleWrite
        val median = shuffleWrite(2) // 50th percentile
        val max = shuffleWrite(4) // 100th percentile
        val ratio = if (median > 0) max / median else 0.0
        if (ratio > 2.0) {
          Some((stageId, ratio))
        } else {
          None
        }
      } else {
        None
      }
    }

    val rows = skewedStages.map { case (stageId, ratio) =>
      Seq(
        stageId,
        f"$ratio%.2f"
      )
    }

    val headers = Seq("Stage ID", "Shuffle Skew Ratio")
    AnalysisResult(
      s"Shuffle Skew Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Shows stages with significant shuffle skew (Max Task Shuffle Write / Median Task Shuffle Write > 2.0) for stages with total shuffle write > 100MB."
    )
  }

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ShuffleSkewAnalyzer only supports single application analysis.")
  }
}
