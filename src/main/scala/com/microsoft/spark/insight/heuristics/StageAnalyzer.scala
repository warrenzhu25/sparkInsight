/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.fetcher.SparkApplicationData
import com.microsoft.spark.insight.fetcher.status._
import com.microsoft.spark.insight.util.{MemoryFormatUtils, Utils}

import scala.collection.mutable.ArrayBuffer

/**
 * Analysis results for a stage.
 */
case class StageAnalysis(stageData: StageData,
                         stageAnalysisResults: Seq[StageAnalysisResult])

trait StageAnalysisResult extends AnalysisResult {

  val stageData: StageData

  val details: Seq[String]

  val name: String

  def format(stageData: StageData): String =
    s"Stage ${stageData.stageId} (Attempt ${stageData.attemptId}) : ${stageData.name}"
}

case class SimpleStageAnalysisResult(name: String,
                                     stageData: StageData,
                                     details: Seq[String]) extends StageAnalysisResult{
  override def toString: String = {
    s"""
       |${format(stageData)} $name Result
       |======================
       |${details.mkString("\n")}
       |""".stripMargin
  }
}

/**
 * Stage analysis result for examining the stage for task skew.
 */
private case class TaskSkewResult(stageData: StageData,
                                  details: Seq[String]) extends StageAnalysisResult {
  val name = "Task Skew"

  override def toString: String = {
    s"""
       |${format(stageData)} $name Result
       |======================
       |${details.mkString("\n")}
       |""".stripMargin
  }
}

/**
 * Stage analysis result for examining the stage for execution memory spill.
 *
 * @param details             information and recommendations from analysis for execution memory spill.
 * @param memoryBytesSpilled  the total amount of execution memory bytes spilled for the stage.
 * @param maxTaskBytesSpilled the maximum number of bytes spilled by a task.
 */
case class ExecutionMemorySpillResult(stageData: StageData,
                                      details: Seq[String],
                                      memoryBytesSpilled: Long,
                                      maxTaskBytesSpilled: Long) extends StageAnalysisResult {
  val name = "Spill"

  override def toString: String = {
    s"""
       |${format(stageData)} $name Result
       |======================
       |${details.mkString("\n")}
       |""".stripMargin
  }
}

/**
 * Stage analysis result for examining the stage for task failures.
 *
 * @param details                 information and recommendations from analysis for task failures.
 */
case class TaskFailureResult(stageData: StageData,
                             details: Seq[String],
                             groupByMessage: Map[String, Int],
                             groupByTarget: Map[String, Int]
                            ) extends StageAnalysisResult {
  val name = "Failure"

  override def toString: String = {
    s"""
       |${format(stageData)} $name Result
       |======================
       |${details.mkString("\n")}
       |Stage Task Failure group by error message
       |======================
       |${groupByMessage.mkString("\n")}
       |Stage Task Failure group by target host
       |======================
       |${groupByTarget.mkString("\n")}
       |""".stripMargin
  }
}

/**
 * Analyzes the stage level metrics for the given application.
 *
 * @param data Spark application data
 */
private[heuristics] class StagesAnalyzer(private val data: SparkApplicationData) {

  import ConfigUtils._

  // serverity thresholds for execution memory spill
  private val executionMemorySpillThresholds: SeverityThresholds =
    DEFAULT_EXECUTION_MEMORY_SPILL_THRESHOLDS

  // severity thresholds for task skew
  private val taskSkewThresholds = DEFAULT_TASK_SKEW_THRESHOLDS

  // severity thresholds for task duration (long running tasks)
  private val taskDurationThresholds = DEFAULT_TASK_DURATION_THRESHOLDS

  // severity thresholds for task failures
  private val taskFailureRateSeverityThresholds = DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS

  // execution memory spill: threshold for processed data, above which some spill is expected
  private val maxDataProcessedThreshold = MemoryFormatUtils.stringToBytes(DEFAULT_MAX_DATA_PROCESSED_THRESHOLD)

  // threshold for ratio of max task duration to stage duration, for flagging task skew
  private val longTaskToStageDurationRatio = DEFAULT_LONG_TASK_TO_STAGE_DURATION_RATIO.toDouble

  // min threshold for median task duration, for flagging task skew
  private val taskDurationMinThreshold = DEFAULT_TASK_SKEW_TASK_DURATION_MIN_THRESHOLD.toLong

  // the maximum number of recommended partitions
  private val maxRecommendedPartitions = DEFAULT_MAX_RECOMMENDED_PARTITIONS.toInt

  // recommendation to give if there is execution memory spill due to too much data being processed.
  // Some amount of spill is expected in this, but alert the users so that they are aware that spill
  // is happening.
  private val executionMemorySpillRecommendation = DEFAULT_EXECUTION_MEMORY_SPILL_LARGE_DATA_RECOMMENDATION

  // recommendation to give if task skew is detected, and input data is read for the stage.
  private val taskSkewInputDataRecommendation = DEFAULT_TASK_SKEW_INPUT_DATA_RECOMMENDATION

  // recommendation to give if task skew is detected, and there is no input data.
  private val taskSkewGenericRecommendation = DEFAULT_TASK_SKEW_GENERIC_RECOMMENDATION

  // recommendation to give if there are long running tasks, and there is a lot of data being
  // processed, and many partitions already. In this case, long running tasks may be expected, but
  // alert the user, in case it is possible to filter out some data.
  private val longTasksLargeDataRecommenation = DEFAULT_LONG_TASKS_LARGE_DATA_RECOMMENDATION

  // recommendation to give if there are long running tasks, a reasonable number of partitions,
  // and not too much data processed. In this case, the tasks are slow.
  private val slowTasksRecommendation = DEFAULT_SLOW_TASKS_RECOMMENDATION

  // recommendation to give if there are long running tasks and relatively few partitions.
  private val longTasksFewPartitionsRecommendation = DEFAULT_LONG_TASKS_FEW_PARTITIONS_RECOMMENDATION

  // recommendation to give if there are long running tasks, input data is being read (and so
  // controlling the number of tasks), and relatively few partitions.
  private val longTasksFewInputPartitionsRecommendation = DEFAULT_LONG_TASKS_FEW_INPUT_PARTITIONS_RECOMMENDATION


  /** @return list of analysis results for all the stages. */
  def getStageAnalysis(): Seq[StageAnalysis] = {
    val appConf: Map[String, String] = data.appConf
    val curNumPartitions = appConf.get(SPARK_SQL_SHUFFLE_PARTITIONS)
      .map(_.toInt).getOrElse(SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT)

    data.stageData.map { stageData =>
      val medianTime = stageData.taskSummary.collect {
        case distribution => distribution.executorRunTime(DISTRIBUTION_MEDIAN_IDX)
      }
      val maxTime = stageData.taskSummary.collect {
        case distribution => distribution.executorRunTime(DISTRIBUTION_MAX_IDX)
      }
      val stageDuration = (stageData.submissionTime, stageData.completionTime) match {
        case (Some(submissionTime), Some(completionTime)) =>
          Some(completionTime.getTime() - submissionTime.getTime())
        case _ => None
      }
      val stageId = stageData.stageId
      val stageTasks = data.taskData.getOrElse(s"$stageId-${stageData.attemptId}", Seq.empty)

      val executionMemorySpillResult = checkForExecutionMemorySpill(stageId, stageData)
      val longTaskResult = checkForLongTasks(stageId, stageData, medianTime, curNumPartitions)
      val taskSkewResult = checkForTaskSkew(stageId, stageData, medianTime, maxTime, stageDuration)
      val stageFailureResult = checkForStageFailure(stageData)
      val taskFailureResult = checkForTaskFailure(stageId, stageData, stageTasks)
      val gcResult = checkForGC(stageId, stageData)

      StageAnalysis(stageData, Seq(executionMemorySpillResult, longTaskResult,
        taskSkewResult, taskFailureResult, stageFailureResult, gcResult).flatten)
    }
  }

  /**
   * Check stage for execution memory spill.
   *
   * @param stageId   stage ID.
   * @param stageData stage data.
   * @return results of execution memory spill analysis for the stage.
   */
  private def checkForExecutionMemorySpill(
                                            stageId: Int,
                                            stageData: StageData): Option[StageAnalysisResult] = {
    val maxData = Seq(stageData.inputBytes, stageData.shuffleReadBytes,
      stageData.shuffleWriteBytes, stageData.outputBytes).max
    val rawSpillSeverity = executionMemorySpillThresholds.severityOf(
      stageData.memoryBytesSpilled / maxData.toDouble)
    val details = new ArrayBuffer[String]
    if (hasSignificantSeverity(rawSpillSeverity)) {
      val memoryBytesSpilled = MemoryFormatUtils.bytesToString(stageData.memoryBytesSpilled)
      val diskBytesSpilled = MemoryFormatUtils.bytesToString(stageData.diskBytesSpilled)
      val ratio = stageData.memoryBytesSpilled.toDouble / stageData.diskBytesSpilled
      details += f"${format(stageData)} has $memoryBytesSpilled/${diskBytesSpilled} (memory/disk) spill. (Ratio $ratio%.3f)"
      // if a lot of data is being processed, the severity is supressed, but give information
      // about the spill to the user, so that they know that spill is happening, and can check
      // if the application can be modified to process less data.
      details += s"${format(stageData)} has ${stageData.numTasks} tasks, " +
        s"${MemoryFormatUtils.bytesToString(stageData.inputBytes)} input read, " +
        s"${MemoryFormatUtils.bytesToString(stageData.shuffleReadBytes)} shuffle read, " +
        s"${MemoryFormatUtils.bytesToString(stageData.shuffleWriteBytes)} shuffle write, " +
        s"${MemoryFormatUtils.bytesToString(stageData.outputBytes)} output."

      if (stageData.taskSummary.isDefined) {
        val summary = stageData.taskSummary.get
        val memorySpill = summary.memoryBytesSpilled(DISTRIBUTION_MEDIAN_IDX).toLong
        val inputBytes = summary.inputMetrics.bytesRead(DISTRIBUTION_MEDIAN_IDX).toLong
        val outputBytes = summary.outputMetrics.bytesWritten(DISTRIBUTION_MEDIAN_IDX).toLong
        val shuffleReadBytes = summary.shuffleReadMetrics.readBytes(DISTRIBUTION_MEDIAN_IDX).toLong
        val shuffleWriteBytes = summary.shuffleWriteMetrics.writeBytes(DISTRIBUTION_MEDIAN_IDX).toLong
        details += s"${format(stageData)} has median task values: " +
          s"${MemoryFormatUtils.bytesToString(memorySpill)} memory spill, " +
          s"${MemoryFormatUtils.bytesToString(inputBytes)} input, " +
          s"${MemoryFormatUtils.bytesToString(shuffleReadBytes)} shuffle read, " +
          s"${MemoryFormatUtils.bytesToString(shuffleWriteBytes)} shuffle write, " +
          s"${MemoryFormatUtils.bytesToString(outputBytes)} output."
      }
    }

    val maxTaskSpill = stageData.taskSummary.collect {
      case distribution => distribution.memoryBytesSpilled(DISTRIBUTION_MAX_IDX)
    }.map(_.toLong).getOrElse(0L)

    if (details.nonEmpty) {
      Some(ExecutionMemorySpillResult(stageData, details,
      stageData.memoryBytesSpilled, maxTaskSpill))
    } else {
      None
    }

  }

  /**
   * Check stage for task skew.
   *
   * @param stageId                stage ID.
   * @param stageData              stage data
   * @param medianTime             median task run time (ms).
   * @param maxTime                maximum task run time (ms).
   * @param stageDuration          stage duration (ms).
   * @return results of task skew analysis for the stage.
   */
  private def checkForTaskSkew(
                                stageId: Int,
                                stageData: StageData,
                                medianTime: Option[Double],
                                maxTime: Option[Double],
                                stageDuration: Option[Long]): Option[TaskSkewResult] = {
    val rawSkewSeverity = (medianTime, maxTime) match {
      case (Some(median), Some(max)) =>
        taskSkewThresholds.severityOf(max / median)
      case _ => Severity.NONE
    }
    val maximum = maxTime.getOrElse(0.0D)
    val taskSkewSeverity =
      if (maximum > taskDurationMinThreshold &&
        maximum > longTaskToStageDurationRatio * stageDuration.getOrElse(Long.MaxValue)) {
        rawSkewSeverity
      } else {
        Severity.NONE
      }
    val details = new ArrayBuffer[String]

    // add more information about what might be causing skew if skew is being flagged
    // (reported severity is significant), or there is execution memory spill, since skew
    // can also cause execution memory spill.
    val medianStr = Utils.getDuration(medianTime.map(_.toLong).getOrElse(0L))
    val maximumStr = Utils.getDuration(maxTime.map(_.toLong).getOrElse(0L))
    var inputSkewSeverity = Severity.NONE
    if (hasSignificantSeverity(taskSkewSeverity)) {
      details +=
        s"${format(stageData)} has skew in task run time (median is $medianStr, max is $maximumStr)."
    }
    stageData.taskSummary.foreach { summary =>
      checkSkewedData(stageId, summary.memoryBytesSpilled(DISTRIBUTION_MEDIAN_IDX),
        summary.memoryBytesSpilled(DISTRIBUTION_MAX_IDX), "memory bytes spilled", details)

      val input = summary.inputMetrics
      inputSkewSeverity = checkSkewedData(stageId, input.bytesRead(DISTRIBUTION_MEDIAN_IDX),
        input.bytesRead(DISTRIBUTION_MAX_IDX), "task input bytes", details)
      if (hasSignificantSeverity(inputSkewSeverity)) {
        // The stage is reading input data, try to adjust the amount of data to even the partitions
        details += s"${format(stageData)}: ${taskSkewInputDataRecommendation}."
      }

      val output = summary.outputMetrics
      checkSkewedData(stageId, output.bytesWritten(DISTRIBUTION_MEDIAN_IDX),
        output.bytesWritten(DISTRIBUTION_MAX_IDX), "task output bytes", details)

      val shuffleRead = summary.shuffleReadMetrics
      checkSkewedData(stageId, shuffleRead.readBytes(DISTRIBUTION_MEDIAN_IDX),
        shuffleRead.readBytes(DISTRIBUTION_MAX_IDX), "task shuffle read bytes", details)

      val shuffleWrite = summary.shuffleWriteMetrics
      checkSkewedData(stageId, shuffleWrite.writeBytes(DISTRIBUTION_MEDIAN_IDX),
        shuffleWrite.writeBytes(DISTRIBUTION_MAX_IDX), "task shuffle write bytes", details)

      if (hasSignificantSeverity(rawSkewSeverity) && !hasSignificantSeverity(inputSkewSeverity)) {
        details += s"${format(stageData)}: ${taskSkewGenericRecommendation}."
      }
    }

    if(details.nonEmpty) {
      Some(TaskSkewResult(stageData, details))
    } else {
      None
    }
  }

  /**
   * Check for skewed data.
   *
   * @param stageId     stage ID
   * @param median      median data size for tasks.
   * @param maximum     maximum data size for tasks.
   * @param description type of data.
   * @param details     information and recommendations -- any new recommendations
   *                    from analyzing the stage for data skew will be appended.
   */
  private def checkSkewedData(
                               stageId: Int,
                               median: Double,
                               maximum: Double,
                               description: String,
                               details: ArrayBuffer[String]): Severity = {
    val severity = taskSkewThresholds.severityOf(maximum / median)
    if (hasSignificantSeverity(severity)) {
      details += s"Stage $stageId has skew in $description (median is " +
        s"${MemoryFormatUtils.bytesToString(median.toLong)}, " +
        s"max is ${MemoryFormatUtils.bytesToString(maximum.toLong)})."
    }
    severity
  }

  /**
   * Check the stage for long running tasks.
   *
   * @param stageId          stage ID.
   * @param stageData        stage data.
   * @param medianTime       median task run time.
   * @param curNumPartitions number of partitions for the Spark application
   *                         (spark.sql.shuffle.partitions).
   * @return results of long running task analysis for the stage
   */
  private def checkForLongTasks(
                                 stageId: Int,
                                 stageData: StageData,
                                 medianTime: Option[Double],
                                 curNumPartitions: Int): Option[SimpleStageAnalysisResult] = {
    val longTaskSeverity = stageData.taskSummary.map { distributions =>
      taskDurationThresholds.severityOf(distributions.executorRunTime(DISTRIBUTION_MEDIAN_IDX))
    }.getOrElse(Severity.NONE)
    val details = new ArrayBuffer[String]
    if (hasSignificantSeverity(longTaskSeverity)) {
      val runTime = Utils.getDuration(medianTime.map(_.toLong).getOrElse(0L))
      val maxData = Seq(stageData.inputBytes, stageData.shuffleReadBytes, stageData.shuffleWriteBytes,
        stageData.outputBytes).max
      val inputBytes = MemoryFormatUtils.bytesToString(stageData.inputBytes)
      val outputBytes = MemoryFormatUtils.bytesToString(stageData.outputBytes)
      val shuffleReadBytes = MemoryFormatUtils.bytesToString(stageData.shuffleReadBytes)
      val shuffleWriteBytes = MemoryFormatUtils.bytesToString(stageData.shuffleWriteBytes)
      details += s"Stage $stageId has a long median task run time of $runTime."
      details += s"Stage $stageId has ${stageData.numTasks} tasks, $inputBytes input," +
        s" $shuffleReadBytes shuffle read, $shuffleWriteBytes shuffle write, and $outputBytes output."
      if (stageData.numTasks >= maxRecommendedPartitions) {
        if (maxData >= maxDataProcessedThreshold) {
          details += s"Stage $stageId: ${longTasksLargeDataRecommenation}."
        } else {
          details += s"Stage $stageId: ${slowTasksRecommendation}."
        }
      }
      else {
        if (stageData.inputBytes > 0) {
          // The stage is reading input data, try to increase the number of readers
          details += s"Stage $stageId: ${longTasksFewInputPartitionsRecommendation}."
        } else if (stageData.numTasks != curNumPartitions) {
          details += s"Stage $stageId: ${longTasksFewPartitionsRecommendation}."
        }
      }
    }

    if (details.nonEmpty) {
      Some(SimpleStageAnalysisResult("Long Task", stageData, details))
    } else {
      None
    }

  }

  /** Given the severity, return true if the severity is not NONE or LOW. */
  private def hasSignificantSeverity(severity: Severity): Boolean = {
    severity != Severity.NONE && severity != Severity.LOW
  }

  /**
   * Check for stage failure.
   *
   * @param stageData stage data.
   * @return results of stage failure analysis for the stage.
   */
  private def checkForStageFailure(stageData: StageData): Option[StageAnalysisResult] = {
    val details = stageData.failureReason
      .map(reason => s"${format(stageData)} failed: ${getMessage(reason)}")
      .toSeq

    if (details.nonEmpty) {
      Some(SimpleStageAnalysisResult("Stage Failure", stageData, details))
    } else {
      None
    }
  }

  private def format(stageData: StageData): String =
    s"Stage ${stageData.stageId} (Attempt ${stageData.attemptId}) : ${stageData.name}"

  /**
   * Check for failed tasks, including failures caused by OutOfMemory errors, and containers
   * killed by YARN for exceeding memory limits.
   *
   * @param stageId   stage ID.
   * @param stageData stage data.
   * @return result of failed tasks analysis for the stage.
   */
  private def checkForTaskFailure(
                                   stageId: Int,
                                   stageData: StageData,
                                   taskData: Seq[TaskData]
                                 ): Option[TaskFailureResult] = {
    val failedTasks = taskData.filter(_.status == "FAILED")

    if (failedTasks.isEmpty) {
      None
    } else {
      val details = new ArrayBuffer[String]()

      val taskFailureRate = stageData.numFailedTasks.toDouble / stageData.numTasks

      details += f"${format(stageData)} has ${stageData.numFailedTasks} failed tasks of ${stageData.numTasks} (Failure rate ${taskFailureRate}%.3f)."

      Some(TaskFailureResult(
        stageData,
        details,
        groupByMessage(failedTasks),
        groupByTargetHost(failedTasks)))
    }
  }

  private def groupByMessage(failedTasks: Iterable[TaskData]): Map[String, Int] = {
    failedTasks
      .flatMap(_.errorMessage)
      .map(getMessage)
      .toSeq
      .sortWith(_.compareTo(_) < 0)
      .groupBy(identity)
      .mapValues(_.size)
  }

  private def groupByHost(failedTasks: Iterable[TaskData]): Map[String, Int] = {
    failedTasks
      .map(_.host)
      .toSeq
      .sortWith(_.compareTo(_) < 0)
      .groupBy(identity)
      .mapValues(_.size)
  }

  private def groupByTargetHost(failedTasks: Iterable[TaskData]): Map[String, Int] = {
    failedTasks
      .filter(_.errorMessage.isDefined)
      .map(_.errorMessage.get)
      .map(getMessage)
      .flatMap(getTargetHost)
      .toSeq
      .sortWith(_.compareTo(_) < 0)
      .groupBy(identity)
      .mapValues(_.size)
  }

  private def getMessage(errorMessage: String): String = {
    val s = errorMessage.split("\\r\\n|\\n|\\r")

    if (s.length <= 2) {
      errorMessage
    } else {
      s(1)
    }
  }

  private def getTargetHost(errorMessage: String): Option[String] = {
    val s = "Failed to connect to "
    val index = errorMessage.indexOf(s)
    if (index == -1) {
      None
    } else {
      val slashIndex = errorMessage.indexOf("/", index)
      Some(errorMessage.substring(index + s.length, slashIndex))
    }
  }


  /**
   * Check the stage for tasks that failed for a specified error.
   *
   * @param stageId      stage ID.
   * @param stageData    stage data.
   * @param failedTasks  list of failed tasks.
   * @param taskError    the error to check for.
   * @param errorMessage the message/explanation to print if the the specified error is found.
   * @param details      information and recommendations -- any new recommendations
   *                     from analyzing the stage for errors causing tasks to fail will be appended.
   * @return
   */
  private def checkForSpecificTaskError(
                                         stageId: Int,
                                         stageData: StageData,
                                         failedTasks: Iterable[TaskData],
                                         taskError: String,
                                         errorMessage: String,
                                         details: ArrayBuffer[String]): (Int, Severity) = {
    val numTasksWithError = getNumTasksWithError(failedTasks, taskError)
    if (numTasksWithError > 0) {
      details += s"Stage $stageId has $numTasksWithError tasks that failed because " +
        errorMessage
    }
    val severity = taskFailureRateSeverityThresholds.severityOf(numTasksWithError.toDouble / stageData.numTasks)
    (numTasksWithError, severity)
  }

  /**
   * Get the number of tasks that failed with the specified error, using a simple string search.
   *
   * @param tasks list of failed tasks.
   * @param error error to look for.
   * @return number of failed tasks wit the specified error.
   */
  private def getNumTasksWithError(tasks: Iterable[TaskData], error: String): Int = {
    tasks.count(task => task.errorMessage.contains(error))
  }

  /**
   * Check the stage for a high ratio of time spent in GC compared to task run time.
   *
   * @param stageId   stage ID.
   * @param stageData stage data.
   * @return result of GC analysis for the stage.
   */
  private def checkForGC(stageId: Int, stageData: StageData): Option[SimpleStageAnalysisResult] = {
    var gcTime = 0.0D
    var taskTime = 0.0D
    val severity = stageData.taskSummary.map { task =>
      gcTime = task.jvmGcTime(DISTRIBUTION_MEDIAN_IDX)
      taskTime = task.executorRunTime(DISTRIBUTION_MEDIAN_IDX)
      DEFAULT_GC_SEVERITY_A_THRESHOLDS.severityOf(gcTime / taskTime)
    }.getOrElse(Severity.NONE)

    val details = if (hasSignificantSeverity(severity)) {
      Seq(s"Stage ${stageId}: tasks are spending significant time in GC (median task GC time is " +
        s"${Utils.getDuration(gcTime.toLong)}, median task runtime is " +
        s"${Utils.getDuration(gcTime.toLong)}")
    } else {
      Seq.empty
    }

    if (details.nonEmpty) {
      Some(SimpleStageAnalysisResult("GC", stageData, details))
    } else {
      None
    }
  }
}
