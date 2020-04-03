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
import com.microsoft.spark.insight.fetcher.status.{StageData, StageStatus}
import com.microsoft.spark.insight.math.Statistics

/**
 * A heuristic based on metrics for a Spark app's stages.
 *
 * This heuristic reports stage failures, high task failure rates for each stage,
 * and long average executor runtimes for each stage.
 */
object StagesHeuristic extends Heuristic {

  override val evaluators = Seq(StagesEvaluator)

  object StagesEvaluator extends SparkEvaluator {

    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {

      lazy val stageDatas: Seq[StageData] = sparkAppData.stageData

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

      val stageAnalysis = new StagesAnalyzer(sparkAppData).getStageAnalysis()

      val summaryRows = Seq(
        SimpleRowResult("Spark completed stages count", numCompletedStages.toString),
        SimpleRowResult("Spark failed stages count", numFailedStages.toString),
        SimpleRowResult("Spark stage failure rate", f"${stageFailureRate.getOrElse(0.0D)}%.3f")
      )

      stageAnalysis.flatMap(_.stageAnalysisResults)

      /*
      Seq(
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
      */
    }

    def formatStageWithLongRuntime(stageData: StageData, runtime: Long): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (runtime: ${Statistics.readableTimespan(runtime)})"

    def formatStagesWithHighTaskFailureRates(stagesWithHighTaskFailureRates: Seq[(StageData, Double)]): Seq[String] =
      stagesWithHighTaskFailureRates
        .map { case (stageData, taskFailureRate) => formatStageWithHighTaskFailureRate(stageData, taskFailureRate) }

    def formatStageWithHighTaskFailureRate(stageData: StageData, taskFailureRate: Double): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (task failure rate: ${taskFailureRate}%1.3f)"

  }

}
