package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, StageData, StageStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class AppSummaryAnalyzerSuite extends AnyFunSuite {

  test("AppSummaryAnalyzer should generate an app summary") {
    val appInfo = ApplicationInfo(
      id = "app-id",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq(
        ApplicationAttemptInfo(
          attemptId = Some("1"),
          startTime = new Date(0),
          endTime = new Date(10000),
          lastUpdated = new Date(10000),
          duration = 10000,
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val stageData = Seq(
      new StageData(
        status = StageStatus.COMPLETE,
        stageId = 1,
        attemptId = 0,
        numTasks = 10,
        numActiveTasks = 0,
        numCompleteTasks = 10,
        numFailedTasks = 0,
        numKilledTasks = 0,
        numCompletedIndices = 10,
        submissionTime = Some(new Date(1000)),
        firstTaskLaunchedTime = Some(new Date(1000)),
        completionTime = Some(new Date(5000)),
        failureReason = None,
        executorRunTime = 40000,
        executorCpuTime = 40000,
        executorDeserializeTime = 0,
        executorDeserializeCpuTime = 0,
        resultSerializationTime = 0,
        jvmGcTime = 0,
        resultSize = 0,
        diskBytesSpilled = 0,
        memoryBytesSpilled = 0,
        peakExecutionMemory = 0,
        inputBytes = 0,
        inputRecords = 0,
        outputBytes = 0,
        outputRecords = 0,
        shuffleRemoteBlocksFetched = 0,
        shuffleLocalBlocksFetched = 0,
        shuffleFetchWaitTime = 0,
        shuffleRemoteBytesRead = 0,
        shuffleRemoteBytesReadToDisk = 0,
        shuffleLocalBytesRead = 0,
        shuffleReadBytes = 0,
        shuffleReadRecords = 0,
        shuffleCorruptMergedBlockChunks = 0,
        shuffleMergedFetchFallbackCount = 0,
        shuffleMergedRemoteBlocksFetched = 0,
        shuffleMergedLocalBlocksFetched = 0,
        shuffleMergedRemoteChunksFetched = 0,
        shuffleMergedLocalChunksFetched = 0,
        shuffleMergedRemoteBytesRead = 0,
        shuffleMergedLocalBytesRead = 0,
        shuffleRemoteReqsDuration = 0,
        shuffleMergedRemoteReqsDuration = 0,
        shuffleWriteBytes = 0,
        shuffleWriteTime = 0,
        shuffleWriteRecords = 0,
        name = "stage1",
        description = Some("details"),
        details = "details",
        schedulingPool = "default",
        rddIds = Seq(),
        accumulatorUpdates = Seq(),
        tasks = None,
        executorSummary = None,
        speculationSummary = None,
        killedTasksSummary = Map(),
        resourceProfileId = 0,
        peakExecutorMetrics = None,
        taskMetricsDistributions = None,
        executorMetricsDistributions = None,
        isShufflePushEnabled = false,
        shuffleMergersCount = 0
      )
    )

    val sparkAppData = SparkApplicationData(
      appId = "app-id",
      appConf = Map(),
      appInfo = appInfo,
      jobData = Seq(),
      stageData = stageData,
      executorSummaries = Seq(),
      taskData = Map()
    )

    val analysisResult = AppSummaryAnalyzer.analysis(sparkAppData)

    assert(analysisResult.name === s"Spark Application Performance Report for applicationId: ${appInfo.id}")
    assert(analysisResult.rows.nonEmpty)
  }
}
