
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class ConfigDiffAnalyzerSuite extends AnyFunSuite {

  test("ConfigDiffAnalyzer should generate a diff report") {
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

    val appConf1 = Map(
      "spark.executor.cores" -> "2",
      "spark.executor.memory" -> "4g",
      "spark.driver.memory" -> "2g"
    )

    val appConf2 = Map(
      "spark.executor.cores" -> "4",
      "spark.executor.memory" -> "4g",
      "spark.driver.memory" -> "3g",
      "spark.new.config" -> "true"
    )

    val appData1 = SparkApplicationData("app-id-1", appConf1, appInfo, Seq(), Seq(), Seq(), Map())
    val appData2 = SparkApplicationData("app-id-2", appConf2, appInfo, Seq(), Seq(), Seq(), Map())

    val result = ConfigDiffAnalyzer.analysis(appData1, appData2)

    assert(result.name === s"Configuration Diff Report for ${appData1.appInfo.id} and ${appData2.appInfo.id}")
    assert(result.rows.size === 3)
    assert(result.rows(0) === Seq("spark.driver.memory", "2g", "3g"))
    assert(result.rows(1) === Seq("spark.executor.cores", "2", "4"))
    assert(result.rows(2) === Seq("spark.new.config", "", "true"))
  }
}
