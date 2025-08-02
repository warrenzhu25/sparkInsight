package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import scala.concurrent.duration.DurationInt

object MaxExecutorAnalyzer extends Analyzer {
  private val headers = Seq("Config", "Current", "Suggest")
  private val TARGET_DURATION = 2.minutes

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData

    var maxExecutors = 2
    var currentMaxExecutors = 0

    val events = stageData.flatMap { stage =>
      val executorsNeeded = math.min(stage.executorRunTime / TARGET_DURATION.toMillis, stage.numTasks).toInt / 4
      Seq((stage.submissionTime, executorsNeeded), (stage.completionTime, -executorsNeeded))
    }.sortBy(_._1) // Sort events by time

    for ((_, taskDelta) <- events) {
      currentMaxExecutors += taskDelta
      maxExecutors = Math.max(maxExecutors, currentMaxExecutors)
    }

    AnalysisResult("Max Executor", headers, Seq(Seq("maxExecutors", "2", maxExecutors.toString)))
  }
}
