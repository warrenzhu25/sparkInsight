package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import java.util.Date

object InitialExecutorAnalyzer extends Analyzer {
  private val headers = Seq("Config", "Current", "Suggest")

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData
    val appStartTime = sparkAppData.appInfo.attempts.head.startTime
    val time = new Date(appStartTime.getTime + 1000 * 60 * 2)

    val stages = stageData.filter(s =>
      s.submissionTime.nonEmpty &&
      s.submissionTime.get.before(time) &&
        s.completionTime.nonEmpty &&
        s.completionTime.get.after(time))

    stages.foreach(s => println(s"${s.stageId}, ${s.executorRunTime}, ${s.executorRunTime / (1000 * 60 * 3)}"))

    val suggested = stages.map(s => s.executorRunTime / (1000 * 60 * 3)).sum.toString

    AnalysisResult("Initial Executor", headers, Seq(Seq("initialExecutors", "2", suggested)))
  }
}
