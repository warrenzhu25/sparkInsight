/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.fetcher.SparkApplicationData
import com.microsoft.spark.insight.fetcher.status.ExecutorSummary
import com.microsoft.spark.insight.math.Statistics
import com.microsoft.spark.insight.util.MemoryFormatUtils

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

/**
 * A heuristic based on metrics for a Spark app's executors.
 *
 * This heuristic concerns the distribution (min, 25p, median, 75p, max) of key executor metrics including input bytes,
 * shuffle read bytes, shuffle write bytes, storage memory used, and task time. The max-to-median ratio determines the
 * severity of any particular metric.
 */
object ExecutorsHeuristic extends Heuristic {

  import JavaConverters._
  import scala.concurrent.duration._

  override val evaluators = Seq(ExecutorsEvaluator)
  val DEFAULT_MAX_TO_MEDIAN_RATIO_SEVERITY_THRESHOLDS: SeverityThresholds = SeverityThresholds(
    low = math.pow(10, 0.125), // ~1.334
    moderate = math.pow(10, 0.25), // ~1.778
    severe = math.pow(10, 0.5), // ~3.162
    critical = 10,
    ascending = true
  )
  val DEFAULT_IGNORE_MAX_BYTES_LESS_THAN_THRESHOLD: Long = MemoryFormatUtils.stringToBytes("100 MB")
  val DEFAULT_IGNORE_MAX_MILLIS_LESS_THAN_THRESHOLD: Long = Duration(5, MINUTES).toMillis
  val MAX_TO_MEDIAN_RATIO_SEVERITY_THRESHOLDS_KEY: String = "max_to_median_ratio_severity_thresholds"
  val IGNORE_MAX_BYTES_LESS_THAN_THRESHOLD_KEY: String = "ignore_max_bytes_less_than_threshold"
  val IGNORE_MAX_MILLIS_LESS_THAN_THRESHOLD_KEY: String = "ignore_max_millis_less_than_threshold"
  val maxToMedianRatioSeverityThresholds = DEFAULT_MAX_TO_MEDIAN_RATIO_SEVERITY_THRESHOLDS
  val ignoreMaxBytesLessThanThreshold = DEFAULT_IGNORE_MAX_BYTES_LESS_THAN_THRESHOLD
  val ignoreMaxMillisLessThanThreshold = DEFAULT_IGNORE_MAX_MILLIS_LESS_THAN_THRESHOLD

  case class Distribution(min: Long, p25: Long, median: Long, p75: Long, max: Long)

  object ExecutorsEvaluator extends SparkEvaluator {

    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {

      lazy val executorSummaries: Seq[ExecutorSummary] = sparkAppData.executorSummaries

      lazy val totalStorageMemoryAllocated: Long = executorSummaries.map {
        _.maxMemory
      }.sum

      lazy val totalStorageMemoryUsed: Long = executorSummaries.map {
        _.memoryUsed
      }.sum

      lazy val storageMemoryUtilizationRate: Double = totalStorageMemoryUsed.toDouble / totalStorageMemoryAllocated.toDouble

      lazy val storageMemoryUsedDistribution: Distribution =
        Distribution(executorSummaries.map {
          _.memoryUsed
        })

      lazy val taskTimeDistribution: Distribution =
        Distribution(executorSummaries.map {
          _.totalDuration
        })

      lazy val totalTaskTime: Long = executorSummaries.map(_.totalDuration).sum

      lazy val inputBytesDistribution: Distribution =
        Distribution(executorSummaries.map {
          _.totalInputBytes
        })

      lazy val shuffleReadBytesDistribution: Distribution =
        Distribution(executorSummaries.map {
          _.totalShuffleRead
        })

      lazy val shuffleWriteBytesDistribution: Distribution =
        Distribution(executorSummaries.map {
          _.totalShuffleWrite
        })

      val rows = Seq(
        SimpleRowResult(
          "Total executor storage memory allocated",
          MemoryFormatUtils.bytesToString(totalStorageMemoryAllocated)
        ),
        SimpleRowResult(
          "Total executor storage memory used",
          MemoryFormatUtils.bytesToString(totalStorageMemoryUsed)
        ),
        SimpleRowResult(
          "Executor storage memory utilization rate",
          f"${storageMemoryUtilizationRate}%1.3f"
        ),
        SimpleRowResult(
          "Executor storage memory used distribution",
          Distribution.formatDistributionBytes(storageMemoryUsedDistribution)
        ),
        SimpleRowResult(
          "Executor task time distribution",
          Distribution.formatDistributionDuration(taskTimeDistribution)
        ),
        SimpleRowResult(
          "Executor task time sum",
          (totalTaskTime / Statistics.SECOND_IN_MS).toString
        ),
        SimpleRowResult(
          "Executor input bytes distribution",
          Distribution.formatDistributionBytes(inputBytesDistribution)
        ),
        SimpleRowResult(
          "Executor shuffle read bytes distribution",
          Distribution.formatDistributionBytes(shuffleReadBytesDistribution)
        ),
        SimpleRowResult(
          "Executor shuffle write bytes distribution",
          Distribution.formatDistributionBytes(shuffleWriteBytesDistribution)
        )
      )
      Seq(SimpleResult("Executor Summary", rows))
    }

    private[heuristics] def severityOfDistribution(
                                                    distribution: Distribution,
                                                    ignoreMaxLessThanThreshold: Long,
                                                    severityThresholds: SeverityThresholds = maxToMedianRatioSeverityThresholds
                                                  ): Severity = {
      if (distribution.max < ignoreMaxLessThanThreshold) {
        Severity.NONE
      } else if (distribution.median == 0L) {
        severityThresholds.severityOf(Long.MaxValue)
      } else {
        severityThresholds.severityOf(BigDecimal(distribution.max) / BigDecimal(distribution.median))
      }
    }
  }

  object Distribution {
    def apply(values: Seq[Long]): Distribution = {
      val sortedValues = values.sorted
      val sortedValuesAsJava = sortedValues.map(Long.box).to[ArrayBuffer].asJava
      Distribution(
        sortedValues.min,
        p25 = Statistics.percentile(sortedValuesAsJava, 25),
        Statistics.median(sortedValuesAsJava),
        p75 = Statistics.percentile(sortedValuesAsJava, 75),
        sortedValues.max
      )
    }

    def formatDistributionBytes(distribution: Distribution): String =
      formatDistribution(distribution, MemoryFormatUtils.bytesToString)

    def formatDistribution(distribution: Distribution, longFormatter: Long => String, separator: String = ", "): String = {
      val labels = Seq(
        s"min: ${longFormatter(distribution.min)}",
        s"p25: ${longFormatter(distribution.p25)}",
        s"median: ${longFormatter(distribution.median)}",
        s"p75: ${longFormatter(distribution.p75)}",
        s"max: ${longFormatter(distribution.max)}"
      )
      labels.mkString(separator)
    }

    def formatDistributionDuration(distribution: Distribution): String =
      formatDistribution(distribution, Statistics.readableTimespan)
  }

}
