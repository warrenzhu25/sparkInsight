package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.StageData

object AppSummaryAnalyzer extends Analyzer {
  private val headers = Seq("Config", "App1", "App2")

  override def analysis(sparkAppData: SparkApplicationData): AnalysisResult = {
    val stageData = sparkAppData.stageData

    val rows = stageData
      .map(s => getMetrics(s))
      .reduce(combineSum)
      .map { case (k, v) => Metric(k, v).toRow() }
      .toSeq

    AnalysisResult("App Summary", headers, rows)
  }
}
