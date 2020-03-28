package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.fetcher.SparkApplicationData

trait SparkEvaluator {
  def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult]

  protected def getProperty(sparkAppData: SparkApplicationData, key: String): Option[String] =
    sparkAppData.appConf.get(key)
}
