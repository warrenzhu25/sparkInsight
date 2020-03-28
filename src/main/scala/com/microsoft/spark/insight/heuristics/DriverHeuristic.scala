/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.fetcher.SparkApplicationData
import com.microsoft.spark.insight.fetcher.status.ExecutorSummary
import com.microsoft.spark.insight.util.{MemoryFormatUtils, Utils}

import scala.util.Try

/**
 * A heuristic based the driver's configurations and memory used.
 * It checks whether the configuration values specified are within the threshold range.
 * It also analyses the peak JVM memory used and time spent in GC by the job.
 */
class DriverHeuristic()
  extends Heuristic {

  import DriverHeuristic._

  lazy val driverPeakJvmMemoryThresholdString = MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY
  val gcSeverityThresholds: SeverityThresholds = DEFAULT_GC_SEVERITY_THRESHOLDS
  val sparkOverheadMemoryThreshold: SeverityThresholds = DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS
  val sparkExecutorMemoryThreshold: String = DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatProperty(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    var resultDetails = Seq(
      new SimpleResult(
        SPARK_DRIVER_MEMORY_KEY,
        formatProperty(evaluator.driverMemoryBytes.map(MemoryFormatUtils.bytesToString))
      ),
      //Removing driver GC heuristics for now
      //      new HeuristicResultDetails(
      //        "Ratio of time spent in GC to total time", evaluator.ratio.toString
      //      ),
      new SimpleResult(
        SPARK_DRIVER_CORES_KEY,
        formatProperty(evaluator.driverCores.map(_.toString))
      ),
      new SimpleResult(
        SPARK_YARN_DRIVER_MEMORY_OVERHEAD,
        evaluator.sparkYarnDriverMemoryOverhead
      ),
      new SimpleResult(DRIVER_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME, MemoryFormatUtils.bytesToString(evaluator.maxDriverPeakJvmUsedMemory))
    )
    if (evaluator.severityJvmUsedMemory != Severity.NONE) {
      resultDetails = resultDetails :+ new SimpleResult("Driver Peak JVM used Memory", "The allocated memory for the driver (in " + SPARK_DRIVER_MEMORY_KEY + ") is much more than the peak JVM used memory by the driver.")
      resultDetails = resultDetails :+ new SimpleResult(SUGGESTED_SPARK_DRIVER_MEMORY_HEURISTIC_NAME, MemoryFormatUtils.roundOffMemoryStringToNextInteger(MemoryFormatUtils.bytesToString(((1 + BUFFER_FRACTION) * (evaluator.maxDriverPeakJvmUsedMemory + reservedMemory)).toLong)))
    }
    if (evaluator.severityGc != Severity.NONE) {
      resultDetails = resultDetails :+ new SimpleResult("Gc ratio high", "The driver is spending too much time on GC. We recommend increasing the driver memory.")
    }
    if (evaluator.severityDriverCores != Severity.NONE) {
      resultDetails = resultDetails :+ new SimpleResult("Driver Cores", "Please do not specify excessive number of driver cores. Change it in the field : " + SPARK_DRIVER_CORES_KEY)
    }
    if (evaluator.severityDriverMemoryOverhead != Severity.NONE) {
      resultDetails = resultDetails :+ new SimpleResult("Driver Overhead Memory", "Please do not specify excessive amount of overhead memory for Driver. Change it in the field " + SPARK_YARN_DRIVER_MEMORY_OVERHEAD)
    }
    if (evaluator.severityDriverMemory != Severity.NONE) {
      resultDetails = resultDetails :+ new SimpleResult("Spark Driver Memory", "Please do not specify excessive amount of memory for Driver. Change it in the field " + SPARK_DRIVER_MEMORY_KEY)
    }

    HeuristicResult(
      name,
      resultDetails
    )
  }
}

object DriverHeuristic {

  val SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory"
  val SPARK_DRIVER_CORES_KEY = "spark.driver.cores"
  val SPARK_YARN_DRIVER_MEMORY_OVERHEAD = "spark.yarn.driver.memoryOverhead"
  val SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY = "spark.overheadMemory.thresholds.key"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold_key"
  val EXECUTION_MEMORY = "executionMemory"
  val STORAGE_MEMORY = "storageMemory"
  val JVM_USED_MEMORY = "jvmUsedMemory"
  val DRIVER_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME = "Max driver peak JVM used memory"
  val SUGGESTED_SPARK_DRIVER_MEMORY_HEURISTIC_NAME = "Suggested spark.driver.memory"
  val BUFFER_FRACTION = 0.2

  // 300 * FileUtils.ONE_MB (300 * 1024 * 1024)
  val reservedMemory: Long = 314572800
  val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "peak_jvm_memory_threshold"
  val GC_SEVERITY_THRESHOLDS_KEY: String = "gc_severity_threshold"
  val DEFAULT_GC_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.09D, severe = 0.1D, critical = 0.15D, ascending = true)

  val DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS =
    SeverityThresholds(low = MemoryFormatUtils.stringToBytes("2G"), MemoryFormatUtils.stringToBytes("4G"),
      severe = MemoryFormatUtils.stringToBytes("6G"), critical = MemoryFormatUtils.stringToBytes("8G"), ascending = true)

  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"

  class Evaluator(driverHeuristic: DriverHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConf

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val driver: ExecutorSummary = executorSummaries.find(_.id == "driver").getOrElse(null)

    if (driver == null) {
      throw new Exception("No driver found!")
    }
    lazy val DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.25 * (maxDriverPeakJvmUsedMemory + reservedMemory), moderate = 1.5 * (maxDriverPeakJvmUsedMemory + reservedMemory),
        severe = 2 * (maxDriverPeakJvmUsedMemory + reservedMemory), critical = 3 * (maxDriverPeakJvmUsedMemory + reservedMemory), ascending = true)
    lazy val severityJvmUsedMemory: Severity = if (driverMemoryBytes.getOrElse(0L).asInstanceOf[Number].longValue <= MemoryFormatUtils.stringToBytes(driverHeuristic.sparkExecutorMemoryThreshold)) {
      Severity.NONE
    } else {
      MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(driverMemoryBytes.getOrElse(0L).asInstanceOf[Number].longValue)
    }
    lazy val driverMemoryBytes: Option[Long] =
      Try(getProperty(SPARK_DRIVER_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(None)
    lazy val driverCores: Option[Int] =
      Try(getProperty(SPARK_DRIVER_CORES_KEY).map(_.toInt)).getOrElse(None)
    lazy val sparkYarnDriverMemoryOverhead: String = if (getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0").matches("(.*)[0-9]"))
      MemoryFormatUtils.bytesToString(MemoryFormatUtils.stringToBytes(getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0") + "MB")) else getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0")
    lazy val severity: Severity = Severity.max(severityConfThresholds, severityGc, severityJvmUsedMemory)
    //peakJvmMemory calculations
    val maxDriverPeakJvmUsedMemory: Long = 0L
    val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS: SeverityThresholds =
      DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS
    //Gc Calculations
    val ratio: Double = driver.totalGCTime.toDouble / driver.totalDuration.toDouble
    val severityGc = driverHeuristic.gcSeverityThresholds.severityOf(ratio)
    //The following thresholds are for checking if the memory and cores values (driver) are above normal. These thresholds are experimental, and may change in the future.
    val DEFAULT_SPARK_MEMORY_THRESHOLDS =
      SeverityThresholds(low = MemoryFormatUtils.stringToBytes("10G"), moderate = MemoryFormatUtils.stringToBytes("15G"),
        severe = MemoryFormatUtils.stringToBytes("20G"), critical = MemoryFormatUtils.stringToBytes("25G"), ascending = true)
    val DEFAULT_SPARK_CORES_THRESHOLDS =
      SeverityThresholds(low = 5, moderate = 7, severe = 9, critical = 11, ascending = true)
    val severityDriverMemory = DEFAULT_SPARK_MEMORY_THRESHOLDS.severityOf(driverMemoryBytes.getOrElse(0).asInstanceOf[Number].longValue)
    val severityDriverCores = DEFAULT_SPARK_CORES_THRESHOLDS.severityOf(driverCores.getOrElse(0).asInstanceOf[Number].intValue)
    val severityDriverMemoryOverhead = driverHeuristic.sparkOverheadMemoryThreshold.severityOf(MemoryFormatUtils.stringToBytes(sparkYarnDriverMemoryOverhead))
    //Severity for the configuration thresholds
    val severityConfThresholds: Severity = Severity.max(severityDriverCores, severityDriverMemory, severityDriverMemoryOverhead)
    val executorCount = 1 //For driver number of executor is 1

    private def getProperty(key: String): Option[String] = appConfigurationProperties.get(key)
  }

}
