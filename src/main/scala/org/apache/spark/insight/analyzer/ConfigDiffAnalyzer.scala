package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData

import scala.util.matching.Regex

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

  private val RANDOM_PATTERNS: Seq[Regex] = Seq(
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".r, // UUID
    "app-[0-9]{12}-[0-9]{4}".r, // Spark Application ID
    "job-[0-9]{12}-[0-9]{4}".r, // Spark Job ID
    "stage-[0-9]{12}-[0-9]{4}".r, // Spark Stage ID
    "attempt-[0-9]{12}-[0-9]{4}".r, // Spark Attempt ID
    "task-[0-9]{12}-[0-9]{4}".r, // Spark Task ID
    "executor-[0-9]{12}-[0-9]{4}".r, // Spark Executor ID
    "rdd_[0-9]+_[0-9]+".r, // RDD ID
    "temp_[0-9a-f]{32}".r, // Temporary file/directory
    "_[0-9a-f]{32}".r, // Suffix with long hex string
    "[0-9a-f]{32}".r // Long hex string
  )

  private def isRandom(s: String): Boolean = {
    RANDOM_PATTERNS.exists(_.findFirstIn(s).isDefined)
  }

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val conf1 = data1.appConf
    val conf2 = data2.appConf

    val allKeys = conf1.keySet ++ conf2.keySet

    val diffs = allKeys.toSeq.sorted.flatMap { key =>
      val value1 = conf1.get(key)
      val value2 = conf2.get(key)

      if (value1 != value2) {
        if (value1.isDefined && value2.isDefined && isRandom(value1.get) && isRandom(value2.get)) {
          None
        } else {
          val category = CATEGORIZED_PREFIXES.find { case (_, prefixes) =>
            prefixes.exists(prefix => key.startsWith(prefix))
          }.map(_._1).getOrElse("Other")
          Some((category, key, value1.getOrElse(""), value2.getOrElse("")))
        }
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