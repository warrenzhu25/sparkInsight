
package org.apache.spark.insight.fetcher

import org.apache.spark.status.api.v1._

/**
 * Case class to hold all the data for a Spark application.
 *
 * @param appId The application ID.
 * @param appConf The application configuration.
 * @param appInfo The application information.
 * @param jobData A sequence of job data.
 * @param stageData A sequence of stage data.
 * @param executorSummaries A sequence of executor summaries.
 * @param taskData A map of task data.
 */
case class SparkApplicationData(
    appId: String,
    appConf: Map[String, String],
    appInfo: ApplicationInfo,
    jobData: Seq[JobData],
    stageData: Seq[StageData],
    executorSummaries: Seq[ExecutorSummary],
    taskData: Map[String, Seq[TaskData]])
