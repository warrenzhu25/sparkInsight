package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

/**
 * An analyzer that compares two Spark applications.
 */
object AppDiffAnalyzer extends Analyzer {

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val metrics1 = getMetrics(data1)
    val metrics2 = getMetrics(data2)

    val commonKeys = metrics1.keySet.intersect(metrics2.keySet)

    val rows = commonKeys.map { key =>
      val value1 = metrics1(key)
      val value2 = metrics2(key)
      val diff = value2 - value1
      val diffPercentage = if (value1 == 0) "N/A" else s"${(diff * 100.0 / value1)}%"
      Seq(key, value1.toString, value2.toString, s"$diff ($diffPercentage)")
    }.toSeq

    AnalysisResult("App Diff", Seq("Metric", "App1", "App2", "Diff"), rows)
  }

  private def getMetrics(sparkAppData: SparkApplicationData): Map[String, Long] = {
    val stageData = sparkAppData.stageData
    stageData
      .map(s => AppSummaryAnalyzer.getMetrics(s))
      .reduce(AppSummaryAnalyzer.combineSum)
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    // This analyzer requires two applications, so this method is not supported.
    throw new UnsupportedOperationException("AppDiffAnalyzer requires two Spark applications to compare.")
  }
}
