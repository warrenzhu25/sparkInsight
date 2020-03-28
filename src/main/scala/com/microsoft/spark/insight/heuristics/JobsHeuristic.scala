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
import com.microsoft.spark.insight.fetcher.status.JobData
import org.apache.spark.JobExecutionStatus

/**
 * A heuristic based on metrics for a Spark app's jobs.
 *
 * This heuristic reports job failures and high task failure rates for each job.
 */
object JobsHeuristic extends Heuristic {
  override val evaluators = Seq(JobEvaluator)
  /** The default severity thresholds for the rate of an application's jobs failing. */
  val DEFAULT_JOB_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)
  /** The default severity thresholds for the rate of a job's tasks failing. */
  val DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)
  val JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_task_failure_rate_severity_thresholds"
  val jobFailureRateSeverityThresholds: SeverityThresholds =
    DEFAULT_JOB_FAILURE_RATE_SEVERITY_THRESHOLDS
  val taskFailureRateSeverityThresholds = DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS

  object JobEvaluator extends SparkEvaluator {

    def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {

      lazy val jobDatas: Seq[JobData] = sparkAppData.jobData

      lazy val numCompletedJobs: Int = jobDatas.count {
        _.status == JobExecutionStatus.SUCCEEDED
      }

      lazy val numFailedJobs: Int = jobDatas.count {
        _.status == JobExecutionStatus.FAILED
      }

      lazy val failedJobs: Seq[JobData] = jobDatas.filter {
        _.status == JobExecutionStatus.FAILED
      }

      lazy val jobFailureRate: Option[Double] = {
        // Currently, the calculation assumes there are no jobs with UNKNOWN or RUNNING state.
        val numJobs = numCompletedJobs + numFailedJobs
        if (numJobs == 0) None else Some(numFailedJobs.toDouble / numJobs.toDouble)
      }

      lazy val jobsWithHighTaskFailureRates: Seq[(JobData, Double)] =
        jobsWithHighTaskFailureRateSeverities.map { case (jobData, taskFailureRate, _) => (jobData, taskFailureRate) }

      lazy val severity: Severity = Severity.max((jobFailureRateSeverity +: taskFailureRateSeverities): _*)

      lazy val jobFailureRateSeverity: Severity =
        jobFailureRateSeverityThresholds.severityOf(jobFailureRate.getOrElse[Double](0.0D))

      lazy val jobsWithHighTaskFailureRateSeverities: Seq[(JobData, Double, Severity)] =
        jobsAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.getValue > Severity.MODERATE.getValue }

      lazy val jobsAndTaskFailureRateSeverities: Seq[(JobData, Double, Severity)] = for {
        jobData <- jobDatas
        (taskFailureRate, severity) = taskFailureRateAndSeverityOf(jobData)
      } yield (jobData, taskFailureRate, severity)

      lazy val taskFailureRateSeverities: Seq[Severity] =
        jobsAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }

      def formatFailedJob(jobData: JobData): String = f"job ${jobData.jobId}, ${jobData.name}"

      def formatJobsWithHighTaskFailureRates(jobsWithHighTaskFailureRates: Seq[(JobData, Double)]): Seq[String] =
        jobsWithHighTaskFailureRates
          .map { case (jobData, taskFailureRate) => formatJobWithHighTaskFailureRate(jobData, taskFailureRate) }

      def formatJobWithHighTaskFailureRate(jobData: JobData, taskFailureRate: Double): String =
        f"job ${jobData.jobId}, ${jobData.name} (task failure rate: ${taskFailureRate}%1.3f)"

      def taskFailureRateAndSeverityOf(jobData: JobData): (Double, Severity) = {
        val taskFailureRate = taskFailureRateOf(jobData).getOrElse(0.0D)
        (taskFailureRate, taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
      }

      def taskFailureRateOf(jobData: JobData): Option[Double] = {
        // Currently, the calculation doesn't include skipped or active tasks.
        val numCompletedTasks = jobData.numCompletedTasks
        val numFailedTasks = jobData.numFailedTasks
        val numTasks = numCompletedTasks + numFailedTasks
        if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
      }

      Seq(
        SimpleResult("Spark completed jobs count", numCompletedJobs.toString),
        SimpleResult("Spark failed jobs count", numFailedJobs.toString),
        MultipleValuesResult("Spark failed jobs list", failedJobs.map(formatFailedJob)),
        SimpleResult("Spark job failure rate", f"${jobFailureRate.getOrElse(0.0D)}%.3f"),
        MultipleValuesResult(
          "Spark jobs with high task failure rates",
          formatJobsWithHighTaskFailureRates(jobsWithHighTaskFailureRates)
        ))
    }
  }

}
