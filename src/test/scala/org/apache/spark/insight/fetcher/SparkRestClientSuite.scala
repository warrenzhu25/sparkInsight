package org.apache.spark.insight.fetcher

import org.apache.spark.status.api.v1._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SparkRestClientSuite extends AnyFunSuite with MockitoSugar {

  test("SparkRestClient should fetch application data") {
    val restClient = mock[SparkRestClient]
    val appData = SparkApplicationData(
      appId = "app-id",
      appConf = Map(),
      appInfo = ApplicationInfo(
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
      ),
      jobData = Seq(),
      stageData = Seq(),
      executorSummaries = Seq(),
      taskData = Map()
    )
    when(restClient.fetchData("http://localhost:18080/history/app-id/1")).thenReturn(Future.successful(appData))

    val future = restClient.fetchData("http://localhost:18080/history/app-id/1")
    future.foreach { data =>
      assert(data.appId === "app-id")
    }
  }
}