package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.TaskStatus

object FailedTaskAnalyzer extends Analyzer {

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val failedTasks = data.taskData.values.flatten.filter(_.status == TaskStatus.FAILED.toString)

    val groupedFailures = failedTasks
      .groupBy(_.errorMessage.getOrElse("Unknown Error"))
      .mapValues(_.size)
      .toSeq
      .sortBy(-_._2)

    val rows = groupedFailures.map { case (reason, count) =>
      Seq(reason, count.toString)
    }

    val headers = Seq("Failure Reason", "Count")
    AnalysisResult(
      s"Failed Task Analysis for ${data.appInfo.id}",
      headers,
      rows,
      "Groups failed tasks by exception and error message."
    )
  }
}
