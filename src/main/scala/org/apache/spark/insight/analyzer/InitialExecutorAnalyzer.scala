package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import java.util.Date
import scala.concurrent.duration.DurationInt

object InitialExecutorAnalyzer extends Analyzer {
  private val headers = Seq("Config", "Current", "Suggest")
  private val TARGET_DURATION = 2.minutes

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData
    val appStartTime = sparkAppData.appInfo.attempts.head.startTime
    val time = new Date(appStartTime.getTime + 1000 * 60 * 2)

    val stages = stageData.filter(s =>
      s.submissionTime.nonEmpty &&
      s.submissionTime.get.before(time) &&
        s.completionTime.nonEmpty &&
        s.completionTime.get.after(time))

    stages.foreach(s => println(s"${s.stageId}, ${s.executorRunTime}, ${s.executorRunTime / (TARGET_DURATION.toMillis)}"))

    val suggested = stages.map(s => math.min(s.executorRunTime / TARGET_DURATION.toMillis, s.numTasks) / 4)
      .sum

    AnalysisResult("Initial Executor", headers, Seq(Seq("initialExecutors", "2", suggested.toString)))
  }
}
