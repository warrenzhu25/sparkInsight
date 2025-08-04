
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

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
        (minute, runningExecutors)
    }

    val mergedRows = new ListBuffer[Seq[String]]()
    if (data.nonEmpty) {
      var startMinute = data.head._1
      var currentCount = data.head._2
      for (i <- 1 until data.length) {
        val (minute, count) = data(i)
        if (count != currentCount) {
          val timeRange = if (startMinute == minute - 1) startMinute.toString else s"$startMinute-${minute - 1}"
          mergedRows += Seq(timeRange, currentCount.toString)
          startMinute = minute
          currentCount = count
        }
      }
      val lastMinute = data.last._1
      val timeRange = if (startMinute == lastMinute) startMinute.toString else s"$startMinute-$lastMinute"
      mergedRows += Seq(timeRange, currentCount.toString)
    }

    val headers = Seq("Time (minutes)", "Running Executors")
    AnalysisResult(
      s"Executor Analysis for ${appInfo.id}",
      headers,
      mergedRows.toSeq,
      "Shows the number of running executors at one-minute intervals."
    )
  }
}
