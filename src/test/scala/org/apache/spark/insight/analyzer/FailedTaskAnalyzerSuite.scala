package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1._
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class FailedTaskAnalyzerSuite extends AnyFunSuite {

  test("FailedTaskAnalyzer should group failed tasks by reason") {
    val appInfo = ApplicationInfo(
      id = "app-id-1",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq(
        ApplicationAttemptInfo(
          attemptId = Some("1"),
          startTime = new Date(0),
          endTime = new Date(1),
          lastUpdated = new Date(1),
          duration = 1,
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val tasks = Map(
      0L -> new TaskData(0, 0, 0, 0, new Date(0), Some(new Date(0)), Some(1L), "executor-1", "host-1", "FAILED", "PROCESS_LOCAL", false, Seq(), Some("Error 1\nstacktrace"), None, Map(), 0, 0),
      1L -> new TaskData(1, 1, 1, 1, new Date(0), Some(new Date(0)), Some(1L), "executor-2", "host-2", "FAILED", "PROCESS_LOCAL", false, Seq(), Some("Error 1\nstacktrace"), None, Map(), 0, 0),
      2L -> new TaskData(2, 2, 2, 2, new Date(0), Some(new Date(0)), Some(1L), "executor-3", "host-3", "FAILED", "PROCESS_LOCAL", false, Seq(), Some("Error 2\nstacktrace"), None, Map(), 0, 0)
    )

    val stageData = Seq(
      new StageData(
        status = StageStatus.COMPLETE,
        stageId = 1,
        attemptId = 0,
        numTasks = 3,
        numActiveTasks = 0,
        numCompleteTasks = 0,
        numFailedTasks = 3,
        numKilledTasks = 0,
        numCompletedIndices = 0,
        submissionTime = Some(new Date(0)),
        firstTaskLaunchedTime = Some(new Date(0)),
        completionTime = Some(new Date(1)),
        failureReason = None,
        executorDeserializeTime = 0,
        executorDeserializeCpuTime = 0,
        executorRunTime = 1,
        executorCpuTime = 1,
        resultSize = 0,
        jvmGcTime = 1,
        resultSerializationTime = 0,
        memoryBytesSpilled = 0,
        diskBytesSpilled = 0,
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
        description = None,
        details = "details",
        schedulingPool = "pool",
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

    val taskData = Map("1" -> tasks.values.toSeq)
    val appData = SparkApplicationData("app-id-1", Map(), appInfo, Seq(), stageData, Seq(), taskData)
    val result = FailedTaskAnalyzer.analysis(appData)

    assert(result.rows.size === 2)
    assert(result.rows.head(0) === "Error 1")
    assert(result.rows.head(1) === "2")
    assert(result.rows(1)(0) === "Error 2")
    assert(result.rows(1)(1) === "1")
  }
}
