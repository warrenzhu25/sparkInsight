
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * An analyzer that compares the number of running executors in two Spark applications.
 */
object ExecutorDiffAnalyzer extends Analyzer {

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val rows1 = getMergedRows(data1)
    val rows2 = getMergedRows(data2)

    val maxRows = Math.max(rows1.size, rows2.size)
    val paddedRows1 = rows1.padTo(maxRows, Seq("", ""))
    val paddedRows2 = rows2.padTo(maxRows, Seq("", ""))

    val rows = (paddedRows1 zip paddedRows2).map { case (row1, row2) =>
      row1 ++ row2
    }

    val headers = Seq("Time (minutes) App1", "Running Executors App1", "Time (minutes) App2", "Running Executors App2")
    AnalysisResult(
      s"Executor Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      headers,
      rows,
      "Shows the number of running executors at one-minute intervals."
    )
  }

  private def getMergedRows(sparkAppData: SparkApplicationData): Seq[Seq[String]] = {
    val appInfo = sparkAppData.appInfo
    if (appInfo.attempts.isEmpty) {
      return Seq.empty
    }
    val startTime = appInfo.attempts.head.startTime.getTime
    var endTime = appInfo.attempts.head.endTime.getTime
    if (endTime < startTime) {
      endTime = System.currentTimeMillis()
    }
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
      val timeRange = if (startMinute == lastMinute) lastMinute.toString else s"$startMinute-$lastMinute"
      mergedRows += Seq(timeRange, currentCount.toString)
    }
    mergedRows.toSeq
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ExecutorDiffAnalyzer requires two Spark applications to compare.")
  }
}
