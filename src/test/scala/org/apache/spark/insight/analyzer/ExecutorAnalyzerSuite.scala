package org.apache.spark.insight.analyzer

import java.util.concurrent.TimeUnit
import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, ExecutorSummary}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class ExecutorAnalyzerSuite extends AnyFunSuite {

  test("ExecutorAnalyzer should calculate running executors") {
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
          endTime = new Date(TimeUnit.MINUTES.toMillis(3)),
          lastUpdated = new Date(TimeUnit.MINUTES.toMillis(3)),
          duration = TimeUnit.MINUTES.toMillis(3),
          sparkUser = "test-user",
          completed = true,
          appSparkVersion = "3.5.0"
        )
      )
    )

    val executorSummaries = Seq(
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
        addTime = new Date(0),
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
      ),
      new ExecutorSummary(
        id = "2",
        hostPort = "localhost:5678",
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
        addTime = new Date(TimeUnit.MINUTES.toMillis(1)),
        removeTime = None,
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

    val sparkAppData = SparkApplicationData(
      appId = "app-id",
      appConf = Map(),
      appInfo = appInfo,
      jobData = Seq(),
      stageData = Seq(),
      executorSummaries = executorSummaries,
      taskData = Map()
    )

    val analysisResult = ExecutorAnalyzer.analysis(sparkAppData)

    assert(analysisResult.name === s"Executor Analysis for ${appInfo.id}")
    assert(analysisResult.rows.size === 3)
    assert(analysisResult.rows(0) === Seq("0", "1"))
    assert(analysisResult.rows(1) === Seq("1", "2"))
    assert(analysisResult.rows(2) === Seq("2-3", "1"))
  }
}