package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

/**
 * An analyzer that compares the Spark configurations of two applications.
 */
object ConfigDiffAnalyzer extends Analyzer {

  private val USEFUL_CONFIG_PREFIXES = Set(
    "spark.executor",
    "spark.driver",
    "spark.sql",
    "spark.memory",
    "spark.storage",
    "spark.shuffle",
    "spark.network",
    "spark.ui",
    "spark.dynamicAllocation",
    "spark.serializer",
    "spark.kryo"
  )

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val conf1 = data1.appConf
    val conf2 = data2.appConf

    val allKeys = conf1.keySet ++ conf2.keySet

    val rows = allKeys.toSeq.sorted.flatMap { key =>
      val value1 = conf1.get(key)
      val value2 = conf2.get(key)

      val isUseful = USEFUL_CONFIG_PREFIXES.exists(prefix => key.startsWith(prefix))
      val isNewOrRemoved = value1.isEmpty || value2.isEmpty

      if (value1 != value2 && (isUseful || isNewOrRemoved)) {
        Some(Seq(key, value1.getOrElse(""), value2.getOrElse("")))
      } else {
        None
      }
    }

    val headers = Seq("Configuration Key", "App1 Value", "App2 Value")
    AnalysisResult(
      s"Configuration Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      headers,
      rows,
      "Shows the differences in Spark configuration between two applications."
    )
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("ConfigDiffAnalyzer requires two Spark applications to compare.")
  }
}