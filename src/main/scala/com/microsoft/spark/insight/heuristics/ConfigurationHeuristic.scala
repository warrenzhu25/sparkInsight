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

import ConfigUtils._

/**
 * A heuristic based on an app's known configuration.
 *
 * The results from this heuristic primarily inform users about key app configuration settings, including
 * driver memory, driver cores, executor cores, executor instances, executor memory, and the serializer.
 *
 * It also checks whether the values specified are within threshold.
 */
object ConfigurationHeuristic extends Heuristic {
  override val evaluators: Seq[SparkEvaluator] = Seq(ConfigEvaluator)
}

object ConfigEvaluator extends SparkEvaluator {
  private val configEvaluators = Seq(
    KyroSerializerEvaluator,
    ShuffleServiceEvaluator,
    ExecutorCoreEvaluator,
    ExecutorMemoryOverheadEvaluator,
    CompressedOopsEvaluator
  )
  override def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult] =
    Seq(ConfigResult(configEvaluators.flatMap(e => e.evaluate(sparkAppData.appConf))))
}

trait SparkConfigEvaluator {
  def evaluate(appConf: Map[String, String]): Option[ConfigRowResult]
  def getProperty(appConf: Map[String, String], key: String): Option[String] =
    appConf.get(key)
}

object KyroSerializerEvaluator extends SparkConfigEvaluator {
  override def evaluate(appConf: Map[String, String]): Option[ConfigRowResult] = {
    val sparkSerializer = getProperty(appConf, SPARK_SERIALIZER_KEY)
    if (sparkSerializer.isEmpty) {
      Some(ConfigRowResult(
        SPARK_SERIALIZER_KEY,
        "",
        "KyroSerializer is Not Enabled.",
        "org.apache.spark.serializer.KryoSerializer"))
    } else {
      None
    }
  }
}

object ShuffleServiceEvaluator extends SparkConfigEvaluator {
  override def evaluate(appConf: Map[String, String]): Option[ConfigRowResult] = {
    val dynamicAllocationEnabled = getProperty(appConf, SPARK_DYNAMIC_ALLOCATION_ENABLED).getOrElse("false").toBoolean
    val shuffleServiceEnabled = getProperty(appConf, SPARK_SHUFFLE_SERVICE_ENABLED).getOrElse("false").toBoolean
    if (dynamicAllocationEnabled && !shuffleServiceEnabled) {
      Some(ConfigRowResult(
        SPARK_DYNAMIC_ALLOCATION_ENABLED,
        "false",
        "Spark shuffle service is not enabled when dynamic allocation is enabled.",
        "true"))
    } else {
      None
    }
  }
}

object ExecutorCoreEvaluator extends SparkConfigEvaluator {
  private val MIN_EXECUTOR_CORES = 2
  private val MAX_EXECUTOR_CORES = 5

  override def evaluate(appConf: Map[String, String]): Option[ConfigRowResult] = {
    val executorCores = getProperty(appConf, SPARK_EXECUTOR_CORES_KEY).getOrElse("1").toInt
    if (executorCores < MIN_EXECUTOR_CORES || executorCores >= MAX_EXECUTOR_CORES) {
      Some(ConfigRowResult(
        SPARK_EXECUTOR_CORES_KEY,
        executorCores.toString,
        "Spark executor cores should be between 2 and 4. Too small cores will have higher overhead, and too many cores lead to poor performance due to HDFS concurrent read issues.",
        "4"))
    } else {
      None
    }
  }
}

object CompressedOopsEvaluator extends SparkConfigEvaluator {
  private val COMPRESSED_OOPS_THRESHOLD = 32

  override def evaluate(appConf: Map[String, String]): Option[ConfigRowResult] = {
    val executorMemory = getProperty(appConf, SPARK_EXECUTOR_MEMORY_KEY).getOrElse("1g")
    if (Utils.byteStringAsGb(executorMemory) < COMPRESSED_OOPS_THRESHOLD) {
      Some(ConfigRowResult(
        "spark.executor.extraJavaOptions",
        "",
        "When executor memory is smaller than 32g, use this option to make pointers be four bytes instead of eight.",
        "-XX:+UseCompressedOops"))
    } else {
      None
    }
  }
}

object ExecutorMemoryOverheadEvaluator extends SparkConfigEvaluator {
  override def evaluate(appConf: Map[String, String]): Option[ConfigRowResult] = {
    val offheapEnabled = getProperty(appConf, "spark.memory.offHeap.enabled").getOrElse("false").toBoolean
    val offHeapSize = getProperty(appConf, "spark.memory.offHeap.size").getOrElse("0").toInt
    val memoryOverhead = getProperty(appConf, SPARK_EXECUTOR_MEMORY_OVERHEAD).getOrElse("384")
    val memory = getProperty(appConf, SPARK_EXECUTOR_MEMORY_KEY).getOrElse("1g")
    val targetOverhead = offHeapSize + Math.min(384, Utils.byteStringAsMb(memory) / 10)
    if (offheapEnabled && (targetOverhead > Utils.byteStringAsMb(memoryOverhead))) {
      Some(ConfigRowResult(
        SPARK_EXECUTOR_MEMORY_OVERHEAD,
        memory,
        "When executor memory off heap is enabled, memory overhead value should add off heap size up.",
        targetOverhead + "m"))
    } else {
      None
    }
  }
}
