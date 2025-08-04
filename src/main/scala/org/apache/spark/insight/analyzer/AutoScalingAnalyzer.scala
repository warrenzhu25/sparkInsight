
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import java.util.Date
import scala.concurrent.duration.DurationInt

/**
 * An analyzer that provides auto-scaling recommendations.
 */
object AutoScalingAnalyzer extends Analyzer {
  private val TARGET_DURATION = 2.minutes

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData
    val appConf = sparkAppData.appConf
    val appStartTime = sparkAppData.appInfo.attempts.head.startTime
    val time = new Date(appStartTime.getTime + 1000 * 60 * 2)

    val initialStages = stageData.filter(s =>
      s.submissionTime.nonEmpty &&
      s.submissionTime.get.before(time) &&
        s.completionTime.nonEmpty &&
        s.completionTime.get.after(time))

    val initialExecutors = initialStages.map(s =>
        math.min(s.executorRunTime / TARGET_DURATION.toMillis, s.numTasks) / 4)
      .sum

    var maxExecutors = 2
    var currentMaxExecutors = 0

    val events = stageData.flatMap { stage =>
      val executorsNeeded = math.min(stage.executorRunTime / TARGET_DURATION.toMillis,
          stage.numTasks).toInt / 4
      Seq((stage.submissionTime, executorsNeeded), (stage.completionTime, -executorsNeeded))
    }.sortBy(_._1) // Sort events by time

    for ((_, taskDelta) <- events) {
      currentMaxExecutors += taskDelta
      maxExecutors = Math.max(maxExecutors, currentMaxExecutors)
    }

    val currentInitialExecutors = appConf.getOrElse("spark.dynamicAllocation.initialExecutors", "N/A")
    val currentMaxExecutorsConf = appConf.getOrElse("spark.dynamicAllocation.maxExecutors", "N/A")

    val headers = Seq("Configuration", "Current", "Suggested", "Description")
    val rows = Seq(
      Seq("Initial Executors", currentInitialExecutors, initialExecutors.toString, "Recommended number of initial executors to provision."),
      Seq("Max Executors", currentMaxExecutorsConf, maxExecutors.toString, "Recommended maximum number of executors for auto-scaling.")
    )

    AnalysisResult(
      s"Auto-Scaling Analysis for ${sparkAppData.appInfo.id}",
      headers,
      rows,
      "Provides recommendations for auto-scaling configuration based on application workload."
    )
  }
}
