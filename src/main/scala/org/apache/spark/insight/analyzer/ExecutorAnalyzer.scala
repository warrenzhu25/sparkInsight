package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.insight.util.CliChart

import java.util.concurrent.TimeUnit

/**
 * An analyzer that shows the number of running executors in one-minute intervals.
 */
object ExecutorAnalyzer extends Analyzer {

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val appInfo = sparkAppData.appInfo
    val startTime = appInfo.attempts.head.startTime.getTime
    val endTime = appInfo.attempts.head.endTime.getTime
    val executorSummaries = sparkAppData.executorSummaries

    val minuteIntervals = (startTime to endTime by TimeUnit.MINUTES.toMillis(1)).map {
      millis =>
        (TimeUnit.MILLISECONDS.toMinutes(millis - startTime), millis)
    }

    val data = minuteIntervals.map {
      case (minute, intervalTime) =>
        val runningExecutors = executorSummaries.count {
          exec =>
            val addTime = exec.addTime.getTime
            val removeTime = exec.removeTime.map(_.getTime).getOrElse(endTime + 1)
            addTime <= intervalTime && intervalTime < removeTime
        }
        (minute.toString, runningExecutors)
    }

    val rows = data.map { case (minute, count) => Seq(minute, count.toString) }
    val chart = CliChart.barChart(data)

    val headers = Seq("Time (minutes)", "Running Executors")
    AnalysisResult(
      s"Executor Analysis for ${appInfo.id}",
      headers,
      rows,
      s"Shows the number of running executors at one-minute intervals.\n\n$chart"
    )
  }
}