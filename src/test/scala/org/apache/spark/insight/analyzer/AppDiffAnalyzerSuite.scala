package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.api.v1.{ApplicationInfo, StageData, StageStatus, AccumulableInfo => UIAccumulableInfo}
import org.scalatest.funsuite.AnyFunSuite

class AppDiffAnalyzerSuite extends AnyFunSuite {

  test("AppDiffAnalyzer should generate a diff report") {
    val stageData1 = new StageData(
      status = StageStatus.ACTIVE,
      stageId = 1,
      attemptId = 1,
      numTasks = 1,
      numActiveTasks = 1,
      numCompleteTasks = 1,
      numFailedTasks = 1,
      numKilledTasks = 1,
      numCompletedIndices = 1,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 1L,
      executorRunTime = 1L,
      executorCpuTime = 1L,
      resultSize = 1L,
      jvmGcTime = 1L,
      resultSerializationTime = 1L,
      memoryBytesSpilled = 1L,
      diskBytesSpilled = 1L,
      peakExecutionMemory = 1L,
      inputBytes = 1L,
      inputRecords = 1L,
      outputBytes = 1L,
      outputRecords = 1L,
      shuffleRemoteBlocksFetched = 1L,
      shuffleLocalBlocksFetched = 1L,
      shuffleFetchWaitTime = 1L,
      shuffleRemoteBytesRead = 1L,
      shuffleRemoteBytesReadToDisk = 1L,
      shuffleLocalBytesRead = 1L,
      shuffleReadBytes = 1L,
      shuffleReadRecords = 1L,
      shuffleCorruptMergedBlockChunks = 1L,
      shuffleMergedFetchFallbackCount = 1L,
      shuffleMergedRemoteBlocksFetched = 1L,
      shuffleMergedLocalBlocksFetched = 1L,
      shuffleMergedRemoteChunksFetched = 1L,
      shuffleMergedLocalChunksFetched = 1L,
      shuffleMergedRemoteBytesRead = 1L,
      shuffleMergedLocalBytesRead = 1L,
      shuffleRemoteReqsDuration = 1L,
      shuffleMergedRemoteReqsDuration = 1L,
      shuffleWriteBytes = 1L,
      shuffleWriteTime = 1L,
      shuffleWriteRecords = 1L,

      name = "stage1",
      description = Some("description"),
      details = "detail",
      schedulingPool = "pool1",

      rddIds = Seq(1),
      accumulatorUpdates = Seq(new UIAccumulableInfo(0L, "acc", None, "value")),
      tasks = None,
      executorSummary = None,
      speculationSummary = None,
      killedTasksSummary = Map.empty,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      peakExecutorMetrics = None,
      taskMetricsDistributions = None,
      executorMetricsDistributions = None,
      isShufflePushEnabled = false,
      shuffleMergersCount = 0
    )
    val appInfo = new ApplicationInfo("app-id", "app-name", None, None, None, None, Seq())
    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo, Seq(), Seq(stageData1), Seq(), Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo, Seq(), Seq(stageData1), Seq(), Map())

    val result = AppDiffAnalyzer.analysis(appData1, appData2)
    assert(result.name == s"Spark Application Diff Report for ${appData1.appInfo.id} and ${appData2.appInfo.id}")
    assert(result.header == Seq("Metric", "App1", "App2", "Diff", "Metric Description"))
    assert(result.rows.nonEmpty)
  }
}