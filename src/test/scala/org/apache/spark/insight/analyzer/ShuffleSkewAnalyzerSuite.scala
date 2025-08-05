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
        shuffleWriteBytes = 2 * 1024 * 1024 * 1024L,
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

    val tasks = Map(
      "1.0" -> Seq(
        new TaskData(
          taskId = 1,
          index = 0,
          attempt = 0,
          partitionId = 0,
          launchTime = new Date(0),
          resultFetchStart = None,
          duration = Some(1),
          executorId = "1",
          host = "localhost",
          status = "SUCCESS",
          taskLocality = "PROCESS_LOCAL",
          speculative = false,
          accumulatorUpdates = Seq(),
          errorMessage = None,
          taskMetrics = Some(
            new TaskMetrics(
              executorDeserializeTime = 0,
              executorDeserializeCpuTime = 0,
              executorRunTime = 1,
              executorCpuTime = 1,
              resultSize = 0,
              jvmGcTime = 0,
              resultSerializationTime = 0,
              memoryBytesSpilled = 0,
              diskBytesSpilled = 0,
              peakExecutionMemory = 0,
              inputMetrics = new InputMetrics(0, 0),
              outputMetrics = new OutputMetrics(0, 0),
              shuffleReadMetrics = new ShuffleReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, new ShufflePushReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)),
              shuffleWriteMetrics = new ShuffleWriteMetrics(0, 0, 1000)
            )
          ),
          executorLogs = Map(),
          schedulerDelay = 0,
          gettingResultTime = 0
        ),
        new TaskData(
          taskId = 2,
          index = 1,
          attempt = 0,
          partitionId = 1,
          launchTime = new Date(0),
          resultFetchStart = None,
          duration = Some(1),
          executorId = "2",
          host = "localhost",
          status = "SUCCESS",
          taskLocality = "PROCESS_LOCAL",
          speculative = false,
          accumulatorUpdates = Seq(),
          errorMessage = None,
          taskMetrics = Some(
            new TaskMetrics(
              executorDeserializeTime = 0,
              executorDeserializeCpuTime = 0,
              executorRunTime = 1,
              executorCpuTime = 1,
              resultSize = 0,
              jvmGcTime = 0,
              resultSerializationTime = 0,
              memoryBytesSpilled = 0,
              diskBytesSpilled = 0,
              peakExecutionMemory = 0,
              inputMetrics = new InputMetrics(0, 0),
              outputMetrics = new OutputMetrics(0, 0),
              shuffleReadMetrics = new ShuffleReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, new ShufflePushReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)),
              shuffleWriteMetrics = new ShuffleWriteMetrics(0, 0, 1000)
            )
          ),
          executorLogs = Map(),
          schedulerDelay = 0,
          gettingResultTime = 0
        ),
        new TaskData(
          taskId = 3,
          index = 2,
          attempt = 0,
          partitionId = 2,
          launchTime = new Date(0),
          resultFetchStart = None,
          duration = Some(1),
          executorId = "3",
          host = "localhost",
          status = "SUCCESS",
          taskLocality = "PROCESS_LOCAL",
          speculative = false,
          accumulatorUpdates = Seq(),
          errorMessage = None,
          taskMetrics = Some(
            new TaskMetrics(
              executorDeserializeTime = 0,
              executorDeserializeCpuTime = 0,
              executorRunTime = 1,
              executorCpuTime = 1,
              resultSize = 0,
              jvmGcTime = 0,
              resultSerializationTime = 0,
              memoryBytesSpilled = 0,
              diskBytesSpilled = 0,
              peakExecutionMemory = 0,
              inputMetrics = new InputMetrics(0, 0),
              outputMetrics = new OutputMetrics(0, 0),
              shuffleReadMetrics = new ShuffleReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, new ShufflePushReadMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)),
              shuffleWriteMetrics = new ShuffleWriteMetrics(0, 0, 5000)
            )
          ),
          executorLogs = Map(),
          schedulerDelay = 0,
          gettingResultTime = 0
        )
      )
    )

    val appData = SparkApplicationData("app-id-1", Map(), appInfo, Seq(), stageData, Seq(), tasks)
    val result = ShuffleSkewAnalyzer.analysis(appData)

    assert(result.rows.size === 1)
    assert(result.rows.head(0) === "1.0")
    assert(result.rows.head(1) === "3")
  }
}
