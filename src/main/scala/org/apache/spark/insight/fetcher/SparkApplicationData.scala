package org.apache.spark.insight.fetcher

import org.apache.spark.status.api.v1._

case class SparkApplicationData(
    appId: String,
    appConf: Map[String, String],
    appInfo: ApplicationInfo,
    jobData: Seq[JobData],
    stageData: Seq[StageData],
    executorSummaries: Seq[ExecutorSummary],
    taskData: Map[String, Seq[TaskData]])
