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

  private def combineSum(left: Map[String, Long], right: Map[String, Long]) = {
    left.keySet.union(right.keySet).map(k => k -> (left.getOrElse(k, 0L) + right.getOrElse(k, 0L))).toMap
  }

  private def getMetrics(stageData: StageData) = {
    stageData.getClass.getDeclaredFields
      .filter(f => f.getType == java.lang.Long.TYPE)
      .map(f => {
        f.setAccessible(true)
        f.getName -> f.get(stageData).asInstanceOf[Long]
      })
      .toMap
  }
}
