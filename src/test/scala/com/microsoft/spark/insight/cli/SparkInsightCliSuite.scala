
package com.microsoft.spark.insight.cli

import org.apache.spark.insight.fetcher.{Fetcher, SparkApplicationData}
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any

import java.util.Date

class SparkInsightCliSuite extends AnyFunSuite with MockitoSugar {

  test("RunCommand should call diff analyzers when app name is provided") {
    val appInfo1 = ApplicationInfo("app-id-1", "test-app", None, None, None, None, Seq(ApplicationAttemptInfo(Some("1"), new Date(0), new Date(10000), new Date(10000), 10000, "test-user", true, "3.5.0")))
    val appInfo2 = ApplicationInfo("app-id-2", "test-app", None, None, None, None, Seq(ApplicationAttemptInfo(Some("1"), new Date(0), new Date(10000), new Date(10000), 10000, "test-user", true, "3.5.0")))
    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo1, Seq(), Seq(), Seq(), Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo2, Seq(), Seq(), Seq(), Map())

    val fetcher = mock[Fetcher]
    when(fetcher.getRecentApplications(any[String], any[Option[String]], any[Int])).thenReturn(Seq(appData1, appData2))

    val runCommand = new RunCommand()
    val appNameField = classOf[RunCommand].getDeclaredField("appName")
    appNameField.setAccessible(true)
    appNameField.set(runCommand, "test-app")

    val fetcherField = classOf[RunCommand].getDeclaredField("fetcher")
    fetcherField.setAccessible(true)
    fetcherField.set(runCommand, fetcher)

    runCommand.call()
  }
}
