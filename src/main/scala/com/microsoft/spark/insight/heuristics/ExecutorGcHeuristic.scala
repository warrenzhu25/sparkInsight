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

/**
 * A heuristic based on GC time and CPU run time. It calculates the ratio of the total time a job spends in GC to the total run time of a job and warns if too much time is spent in GC.
 */
object ExecutorGcHeuristic extends Heuristic {
  override val evaluators = Seq(ExecutorsGcEvaluator)
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  /** The ascending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is above normal)
   * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)
  /** The descending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is below normal)
   * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_D_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.04D, severe = 0.03D, critical = 0.01D, ascending = false)

  object ExecutorsGcEvaluator extends SparkEvaluator {
    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      lazy val executorAndDriverSummaries: Seq[ExecutorSummary] = sparkAppData.executorSummaries
      lazy val executorSummaries: Seq[ExecutorSummary] = executorAndDriverSummaries.filterNot(_.id.equals("driver"))
      val (jvmTime, executorRunTimeTotal) = getTimeValues(executorSummaries)

      val ratio: Double = jvmTime.toDouble / executorRunTimeTotal.toDouble

      Seq(
        SimpleResult("GC time to Executor Run time ratio", ratio.toString),
        SimpleResult("Total GC time", jvmTime.toString),
        SimpleResult("Total Executor Runtime", executorRunTimeTotal.toString)
      )
    }

    /**
     * returns the total JVM GC Time and total executor Run Time across all stages
     *
     * @param executorSummaries
     * @return
     */
    private def getTimeValues(executorSummaries: Seq[ExecutorSummary]): (Long, Long) = {
      var jvmGcTimeTotal: Long = 0
      var executorRunTimeTotal: Long = 0
      executorSummaries.foreach(executorSummary => {
        jvmGcTimeTotal += executorSummary.totalGCTime
        executorRunTimeTotal += executorSummary.totalDuration
      })
      (jvmGcTimeTotal, executorRunTimeTotal)
    }
  }


}

