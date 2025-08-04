
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

/**
 * An analyzer that compares the Spark configurations of two applications.
 */
object ConfigDiffAnalyzer extends Analyzer {

  private val CATEGORIZED_PREFIXES = Map(
    "Scheduling" -> Set("spark.scheduler", "spark.speculation"),
    "Memory Management" -> Set("spark.memory", "spark.storage"),
    "Shuffle Behavior" -> Set("spark.shuffle"),
    "Execution" -> Set("spark.executor", "spark.driver", "spark.task"),
    "SQL" -> Set("spark.sql"),
    "Network" -> Set("spark.network"),
    "UI" -> Set("spark.ui"),
    "Dynamic Allocation" -> Set("spark.dynamicAllocation"),
    "Serialization" -> Set("spark.serializer", "spark.kryo")
  )

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val conf1 = data1.appConf
    val conf2 = data2.appConf

    val allKeys = conf1.keySet ++ conf2.keySet

    val diffs = allKeys.toSeq.sorted.flatMap { key =>
      val value1 = conf1.get(key)
      val value2 = conf2.get(key)

      if (value1 != value2) {
        val category = CATEGORIZED_PREFIXES.find { case (_, prefixes) =>
          prefixes.exists(prefix => key.startsWith(prefix))
        }.map(_._1).getOrElse("Other")
        Some((category, key, value1.getOrElse(""), value2.getOrElse("")))
      } else {
        None
      }
    }

    val groupedDiffs = diffs.groupBy(_._1).toSeq.sortBy(_._1)

    val rows = groupedDiffs.flatMap { case (category, diffs) =>
      Seq(Seq(s"[$category]", "", "")) ++ diffs.map { case (_, key, v1, v2) =>
        Seq(key, v1, v2)
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
