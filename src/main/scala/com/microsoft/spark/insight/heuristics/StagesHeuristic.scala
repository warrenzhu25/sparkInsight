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
import com.microsoft.spark.insight.fetcher.status.{ExecutorSummary, StageData, StageStatus}
import com.microsoft.spark.insight.math.Statistics

import scala.concurrent.duration
import scala.concurrent.duration.Duration

/**
 * A heuristic based on metrics for a Spark app's stages.
 *
 * This heuristic reports stage failures, high task failure rates for each stage, and long average executor runtimes for
 * each stage.
 */
object StagesHeuristic extends Heuristic {

  override val evaluators = Seq(StagesEvaluator)
  /** The default severity thresholds for the rate of an application's stages failing. */
  val DEFAULT_STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)
  /** The default severity thresholds for the rate of a stage's tasks failing. */
  val DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)
  /** The default severity thresholds for a stage's runtime. */
  val DEFAULT_STAGE_RUNTIME_MILLIS_SEVERITY_THRESHOLDS = SeverityThresholds(
    low = Duration("15min").toMillis,
    moderate = Duration("30min").toMillis,
    severe = Duration("45min").toMillis,
    critical = Duration("60min").toMillis,
    ascending = true
  )
  val STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_task_failure_rate_severity_thresholds"
  val STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY = "stage_runtime_minutes_severity_thresholds"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"

  object StagesEvaluator extends SparkEvaluator {

    lazy val stageFailureRateSeverityThresholds = DEFAULT_STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS

    lazy val taskFailureRateSeverityThresholds = DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS

    lazy val stageRuntimeMillisSeverityThresholds = DEFAULT_STAGE_RUNTIME_MILLIS_SEVERITY_THRESHOLDS

    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {

      lazy val stageDatas: Seq[StageData] = sparkAppData.stageData

      lazy val executorSummaries: Seq[ExecutorSummary] = sparkAppData.executorSummaries

      lazy val numCompletedStages: Int = stageDatas.count {
        _.status == StageStatus.COMPLETE
      }

      lazy val numFailedStages: Int = stageDatas.count {
        _.status == StageStatus.FAILED
      }

      lazy val stageFailureRate: Option[Double] = {
        val numStages = numCompletedStages + numFailedStages
        if (numStages == 0) None else Some(numFailedStages.toDouble / numStages.toDouble)
      }

      lazy val stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)] =
        stagesAndAverageExecutorRuntimeSeverities
          .collect { case (stageData, runtime, severity) if severity.getValue > Severity.MODERATE.getValue => (stageData, runtime) }

      lazy val severity: Severity = Severity.max((stageFailureRateSeverity +: (taskFailureRateSeverities ++ runtimeSeverities)): _*)

      lazy val stageFailureRateSeverity: Severity =
        stageFailureRateSeverityThresholds.severityOf(stageFailureRate.getOrElse[Double](0.0D))

      lazy val stagesAndTaskFailureRateSeverities: Seq[(StageData, Double, Severity)] = for {
        stageData <- stageDatas
        (taskFailureRate, severity) = taskFailureRateAndSeverityOf(stageData)
      } yield (stageData, taskFailureRate, severity)

      lazy val taskFailureRateSeverities: Seq[Severity] =
        stagesAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }

      lazy val executorInstances = executorSummaries.size
      lazy val stagesAndAverageExecutorRuntimeSeverities: Seq[(StageData, Long, Severity)] = for {
        stageData <- stageDatas
        (runtime, severity) = averageExecutorRuntimeAndSeverityOf(stageData, executorInstances)
      } yield (stageData, runtime, severity)

      lazy val runtimeSeverities: Seq[Severity] = stagesAndAverageExecutorRuntimeSeverities.map { case (_, _, severity) => severity }

      val stageAnalysis = new StagesAnalyzer(sparkAppData).getStageAnalysis()

      Seq(
        SimpleResult("Spark completed stages count", numCompletedStages.toString),
        SimpleResult("Spark failed stages count", numFailedStages.toString),
        SimpleResult("Spark stage failure rate", f"${stageFailureRate.getOrElse(0.0D)}%.3f"),
        MultipleValuesResult(
          "Spark stages with long average executor runtimes",
          formatStagesWithLongAverageExecutorRuntimes(stagesWithLongAverageExecutorRuntimes)
        ),
        MultipleValuesResult(
          "Spark task failure result", stageAnalysis.map(_.taskFailureResult.toString)
        ),
        MultipleValuesResult(
          "Spark stage failure result", stageAnalysis.flatMap(_.stageFailureResult.details)
        ),
        MultipleValuesResult(
          "Spark task skew result", stageAnalysis.flatMap(_.taskSkewResult.details)
        ),
        MultipleValuesResult(
          "Spark long task result", stageAnalysis.flatMap(_.longTaskResult.details)
        ),
        MultipleValuesResult(
          "Spark task spill result", stageAnalysis.flatMap(_.executionMemorySpillResult.details)
        )
      )
    }

    def taskFailureRateAndSeverityOf(stageData: StageData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(stageData).getOrElse(0.0D)
      (taskFailureRate, taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
    }

    def taskFailureRateOf(stageData: StageData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompleteTasks = stageData.numCompleteTasks
      val numFailedTasks = stageData.numFailedTasks
      val numTasks = numCompleteTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }

    def averageExecutorRuntimeAndSeverityOf(stageData: StageData, executorInstances: Int): (Long, Severity) = {
      val averageExecutorRuntime = stageData.executorRunTime / executorInstances
      (averageExecutorRuntime, stageRuntimeMillisSeverityThresholds.severityOf(averageExecutorRuntime))
    }

    def formatStagesWithLongAverageExecutorRuntimes(stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)]): Seq[String] =
      stagesWithLongAverageExecutorRuntimes
        .map { case (stageData, runtime) => formatStageWithLongRuntime(stageData, runtime) }

    def formatStageWithLongRuntime(stageData: StageData, runtime: Long): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (runtime: ${Statistics.readableTimespan(runtime)})"

    def formatStagesWithHighTaskFailureRates(stagesWithHighTaskFailureRates: Seq[(StageData, Double)]): Seq[String] =
      stagesWithHighTaskFailureRates
        .map { case (stageData, taskFailureRate) => formatStageWithHighTaskFailureRate(stageData, taskFailureRate) }

    def formatStageWithHighTaskFailureRate(stageData: StageData, taskFailureRate: Double): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (task failure rate: ${taskFailureRate}%1.3f)"

    def minutesSeverityThresholdsToMillisSeverityThresholds(
                                                             minutesSeverityThresholds: SeverityThresholds
                                                           ): SeverityThresholds = SeverityThresholds(
      Duration(minutesSeverityThresholds.low.longValue, duration.MINUTES).toMillis,
      Duration(minutesSeverityThresholds.moderate.longValue, duration.MINUTES).toMillis,
      Duration(minutesSeverityThresholds.severe.longValue, duration.MINUTES).toMillis,
      Duration(minutesSeverityThresholds.critical.longValue, duration.MINUTES).toMillis,
      minutesSeverityThresholds.ascending
    )
  }

}
