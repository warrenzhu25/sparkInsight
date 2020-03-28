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
import com.microsoft.spark.insight.util.Utils

/**
 * A heuristic based on an app's known configuration.
 *
 * The results from this heuristic primarily inform users about key app configuration settings, including
 * driver memory, driver cores, executor cores, executor instances, executor memory, and the serializer.
 *
 * It also checks whether the values specified are within threshold.
 */
object ConfigurationHeuristic extends Heuristic {
  override val evaluators = Seq(
    KyroSerializerEvaluator,
    ShuffleServiceEvaluator,
    ExecutorCoreEvaluator,
    ExecutorMemoryOverheadEvaluator,
    CompressedOopsEvaluator
  )
  val SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"
  val SPARK_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
  val SPARK_DRIVER_CORES_KEY = "spark.driver.cores"
  val SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors"
  val SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.executor.memoryOverhead"
  val THRESHOLD_MIN_EXECUTORS: Int = 1
  val THRESHOLD_MAX_EXECUTORS: Int = 900

  object KyroSerializerEvaluator extends SparkEvaluator {
    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      val sparkSerializer = getProperty(sparkAppData, SPARK_SERIALIZER_KEY)
      if (sparkSerializer.isEmpty) {
        Seq(SingleValueResult(
          SPARK_SERIALIZER_KEY,
          "",
          "KyroSerializer is Not Enabled.",
          "org.apache.spark.serializer.KryoSerializer"))
      } else {
        Seq.empty
      }
    }
  }

  object ShuffleServiceEvaluator extends SparkEvaluator {
    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      val dynamicAllocationEnabled = getProperty(sparkAppData, SPARK_DYNAMIC_ALLOCATION_ENABLED).getOrElse("false").toBoolean
      val shuffleServiceEnabled = getProperty(sparkAppData, SPARK_SHUFFLE_SERVICE_ENABLED).getOrElse("false").toBoolean
      if (dynamicAllocationEnabled && !shuffleServiceEnabled) {
        Seq(SingleValueResult(
          SPARK_DYNAMIC_ALLOCATION_ENABLED,
          "false",
          "Spark shuffle service is not enabled when dynamic allocation is enabled.",
          "true"))
      } else {
        Seq.empty
      }
    }
  }

  object ExecutorCoreEvaluator extends SparkEvaluator {
    private val MIN_EXECUTOR_CORES = 2
    private val MAX_EXECUTOR_CORES = 5

    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      val executorCores = getProperty(sparkAppData, SPARK_EXECUTOR_CORES_KEY).getOrElse("1").toInt
      if (executorCores < MIN_EXECUTOR_CORES || executorCores >= MAX_EXECUTOR_CORES) {
        Seq(SingleValueResult(
          SPARK_EXECUTOR_CORES_KEY,
          executorCores.toString,
          "Spark executor cores should be between 2 and 4. Too small cores will have higher overhead, and too many cores lead to poor performance due to HDFS concurrent read issues.",
          "4"))
      } else {
        Seq.empty
      }
    }
  }

  object CompressedOopsEvaluator extends SparkEvaluator {
    private val COMPRESSED_OOPS_THRESHOLD = 32

    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      val executorMemory = getProperty(sparkAppData, SPARK_EXECUTOR_MEMORY_KEY).getOrElse("1g")
      if (Utils.byteStringAsGb(executorMemory) < COMPRESSED_OOPS_THRESHOLD) {
        Seq(SingleValueResult(
          "spark.executor.extraJavaOptions",
          "",
          "When executor memory is smaller than 32g, use this option to make pointers be four bytes instead of eight.",
          "-XX:+UseCompressedOops"))
      } else {
        Seq.empty
      }
    }
  }

  object ExecutorMemoryOverheadEvaluator extends SparkEvaluator {
    override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] = {
      val offheapEnabled = getProperty(sparkAppData, "spark.memory.offHeap.enabled").getOrElse("false").toBoolean
      val offHeapSize = getProperty(sparkAppData, "spark.memory.offHeap.size").getOrElse("0").toInt
      val memoryOverhead = getProperty(sparkAppData, SPARK_EXECUTOR_MEMORY_OVERHEAD).getOrElse("384")
      val memory = getProperty(sparkAppData, SPARK_EXECUTOR_MEMORY_KEY).getOrElse("1g")
      val targetOverhead = offHeapSize + Math.min(384, Utils.byteStringAsMb(memory) / 10)
      if (offheapEnabled && (targetOverhead > Utils.byteStringAsMb(memoryOverhead))) {
        Seq(SingleValueResult(
          SPARK_EXECUTOR_MEMORY_OVERHEAD,
          memory,
          "When executor memory off heap is enabled, memory overhead value should add off heap size up.",
          targetOverhead + "m"))
      } else {
        Seq.empty
      }
    }
  }

}
