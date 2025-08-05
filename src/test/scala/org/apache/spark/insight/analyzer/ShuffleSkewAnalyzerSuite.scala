package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1._
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class ShuffleSkewAnalyzerSuite extends AnyFunSuite {

  test("ShuffleSkewAnalyzer should detect shuffle skew") {
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

    val executorSummary = Map(
      "1" -> new ExecutorStageSummary(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1000, 0, 0, 0, false, None, false),
      "2" -> new ExecutorStageSummary(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1000, 0, 0, 0, false, None, false),
      "3" -> new ExecutorStageSummary(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5000, 0, 0, 0, false, None, false)
    )

    val executorMetricsDistributions = Some(new ExecutorMetricsDistributions(
      quantiles = IndexedSeq(0.0, 0.25, 0.5, 0.75, 1.0),
      taskTime = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      failedTasks = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      succeededTasks = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      killedTasks = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      inputBytes = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      inputRecords = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      outputBytes = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      outputRecords = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      shuffleRead = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      shuffleReadRecords = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      shuffleWrite = IndexedSeq(100.0, 100.0, 1000.0, 2000.0, 5000.0),
      shuffleWriteRecords = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      memoryBytesSpilled = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      diskBytesSpilled = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
      peakMemoryMetrics = new ExecutorPeakMetricsDistributions(
        quantiles = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
        executorMetrics = IndexedSeq()
      )
    ))

    val stageData = Seq(
      new StageData(
        status = StageStatus.COMPLETE,
        stageId = 1,
        attemptId = 0,
        numTasks = 3,
        numActiveTasks = 0,
        numCompleteTasks = 3,
        numFailedTasks = 0,
        numKilledTasks = 0,
        numCompletedIndices = 3,
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
        shuffleWriteBytes = 11L * 1024 * 1024 * 1024,
        shuffleWriteTime = 0,
        shuffleWriteRecords = 0,
        name = "stage1",
        description = None,
        details = "details",
        schedulingPool = "pool",
        rddIds = Seq(),
        accumulatorUpdates = Seq(),
        tasks = None,
        executorSummary = Some(executorSummary),
        speculationSummary = None,
        killedTasksSummary = Map(),
        resourceProfileId = 0,
        peakExecutorMetrics = None,
        taskMetricsDistributions = None,
        executorMetricsDistributions = executorMetricsDistributions,
        isShufflePushEnabled = false,
        shuffleMergersCount = 0
      )
    )

    val appData = SparkApplicationData("app-id-1", Map(), appInfo, Seq(), stageData, Seq(), Map())
    val result = ShuffleSkewAnalyzer.analysis(appData)

    assert(result.rows.size === 1)
    assert(result.rows.head(0) === "1.0")
    assert(result.rows.head(1) === "5.00")
    assert(result.rows.head(2) === "4.88 KB")
    assert(result.rows.head(3) === "1000 B")
    assert(result.rows.head(4) === "11.00 GB")
  }
}