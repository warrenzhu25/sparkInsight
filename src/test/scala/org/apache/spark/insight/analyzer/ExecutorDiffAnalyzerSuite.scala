
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, ExecutorSummary}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date
import java.util.concurrent.TimeUnit

class ExecutorDiffAnalyzerSuite extends AnyFunSuite {

  test("ExecutorDiffAnalyzer should generate a diff report") {
    val startTime = 0L
    val endTime = TimeUnit.MINUTES.toMillis(3)

    val appInfo1 = ApplicationInfo(
      id = "app-id-1",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq(
        ApplicationAttemptInfo(
          attemptId = Some("1"),
          startTime = new Date(startTime),
          endTime = new Date(endTime),
          lastUpdated = new Date(endTime),
          duration = endTime - startTime,
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val executorSummaries1 = Seq(
      new ExecutorSummary(
        id = "1",
        hostPort = "localhost:1234",
        isActive = true,
        rddBlocks = 0,
        memoryUsed = 0,
        diskUsed = 0,
        totalCores = 1,
        maxTasks = 1,
        activeTasks = 0,
        failedTasks = 0,
        completedTasks = 0,
        totalTasks = 0,
        totalDuration = 0,
        totalGCTime = 0,
        totalInputBytes = 0,
        totalShuffleRead = 0,
        totalShuffleWrite = 0,
        isBlacklisted = false,
        maxMemory = 0,
        addTime = new Date(startTime),
        removeTime = Some(new Date(TimeUnit.MINUTES.toMillis(2))),
        removeReason = None,
        executorLogs = Map(),
        memoryMetrics = None,
        blacklistedInStages = Set(),
        peakMemoryMetrics = None,
        attributes = Map(),
        resources = Map(),
        resourceProfileId = 0,
        isExcluded = false,
        excludedInStages = Set()
      )
    )

    val appInfo2 = ApplicationInfo(
      id = "app-id-2",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq(
        ApplicationAttemptInfo(
          attemptId = Some("1"),
          startTime = new Date(startTime),
          endTime = new Date(endTime),
          lastUpdated = new Date(endTime),
          duration = endTime - startTime,
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val executorSummaries2 = Seq(
      new ExecutorSummary(
        id = "1",
        hostPort = "localhost:1234",
        isActive = true,
        rddBlocks = 0,
        memoryUsed = 0,
        diskUsed = 0,
        totalCores = 1,
        maxTasks = 1,
        activeTasks = 0,
        failedTasks = 0,
        completedTasks = 0,
        totalTasks = 0,
        totalDuration = 0,
        totalGCTime = 0,
        totalInputBytes = 0,
        totalShuffleRead = 0,
        totalShuffleWrite = 0,
        isBlacklisted = false,
        maxMemory = 0,
        addTime = new Date(startTime),
        removeTime = Some(new Date(endTime)),
        removeReason = None,
        executorLogs = Map(),
        memoryMetrics = None,
        blacklistedInStages = Set(),
        peakMemoryMetrics = None,
        attributes = Map(),
        resources = Map(),
        resourceProfileId = 0,
        isExcluded = false,
        excludedInStages = Set()
      )
    )

    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo1, Seq(), Seq(), executorSummaries1, Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo2, Seq(), Seq(), executorSummaries2, Map())

    val result = ExecutorDiffAnalyzer.analysis(appData1, appData2)

    assert(result.name === s"Executor Diff Report for ${appData1.appInfo.id} and ${appData2.appInfo.id}")
    assert(result.rows.size === 4)
    assert(result.rows(0) === Seq("0", "1", "0", "1"))
    assert(result.rows(1) === Seq("1", "1", "1", "1"))
    assert(result.rows(2) === Seq("2", "0", "2", "1"))
    assert(result.rows(3) === Seq("3", "0", "3", "0"))
  }

  test("ExecutorDiffAnalyzer should handle applications with no attempts") {
    val startTime = 0L
    val endTime = TimeUnit.MINUTES.toMillis(3)

    val appInfo1 = ApplicationInfo(
      id = "app-id-1",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq(
        ApplicationAttemptInfo(
          attemptId = Some("1"),
          startTime = new Date(startTime),
          endTime = new Date(endTime),
          lastUpdated = new Date(endTime),
          duration = endTime - startTime,
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val executorSummaries1 = Seq(
      new ExecutorSummary(
        id = "1",
        hostPort = "localhost:1234",
        isActive = true,
        rddBlocks = 0,
        memoryUsed = 0,
        diskUsed = 0,
        totalCores = 1,
        maxTasks = 1,
        activeTasks = 0,
        failedTasks = 0,
        completedTasks = 0,
        totalTasks = 0,
        totalDuration = 0,
        totalGCTime = 0,
        totalInputBytes = 0,
        totalShuffleRead = 0,
        totalShuffleWrite = 0,
        isBlacklisted = false,
        maxMemory = 0,
        addTime = new Date(startTime),
        removeTime = Some(new Date(TimeUnit.MINUTES.toMillis(2))),
        removeReason = None,
        executorLogs = Map(),
        memoryMetrics = None,
        blacklistedInStages = Set(),
        peakMemoryMetrics = None,
        attributes = Map(),
        resources = Map(),
        resourceProfileId = 0,
        isExcluded = false,
        excludedInStages = Set()
      )
    )

    val appInfo2 = ApplicationInfo(
      id = "app-id-2",
      name = "test-app",
      coresGranted = Some(1),
      maxCores = Some(2),
      coresPerExecutor = Some(1),
      memoryPerExecutorMB = Some(1024),
      attempts = Seq()
    )

    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo1, Seq(), Seq(), executorSummaries1, Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo2, Seq(), Seq(), Seq(), Map())

    val result = ExecutorDiffAnalyzer.analysis(appData1, appData2)

    assert(result.name === s"Executor Diff Report for ${appData1.appInfo.id} and ${appData2.appInfo.id}")
    assert(result.rows.size === 4)
    assert(result.rows(0) === Seq("0", "1", "", ""))
    assert(result.rows(1) === Seq("1", "1", "", ""))
    assert(result.rows(2) === Seq("2", "0", "", ""))
    assert(result.rows(3) === Seq("3", "0", "", ""))
  }
}
