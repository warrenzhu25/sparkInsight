package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.FormatUtils

import java.util.concurrent.TimeUnit

object ShuffleWaitAnalyzer extends Analyzer {

  private val SHUFFLE_WAIT_THRESHOLD = TimeUnit.MINUTES.toMillis(2) // 2 minutes

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val stagesWithHighShuffleWait = data.stageData.filter(_.shuffleFetchWaitTime > SHUFFLE_WAIT_THRESHOLD)

    val rows = stagesWithHighShuffleWait.map { stage =>
      Seq(
        s"${stage.stageId}.${stage.attemptId}",
        stage.name,
        FormatUtils.formatValue(stage.shuffleFetchWaitTime, isTime = true, isNanoTime = false, isSize = false, isRecords = false)
      )
    }

    val headers = Seq("Stage ID", "Name", "Shuffle Wait Time (minutes)")
    AnalysisResult(
      s"Shuffle Wait Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Shows stages with shuffle wait time greater than 2 minutes."
    )
  }
}
