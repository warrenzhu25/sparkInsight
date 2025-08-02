package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationInfo, StageData}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class HtmlReportAnalyzerSuite extends AnyFunSuite with MockitoSugar {

  test("HtmlReportAnalyzer should generate an HTML report") {
    val stageData = mock[StageData]
    when(stageData.stageId).thenReturn(1)
    when(stageData.name).thenReturn("stage1")
    when(stageData.status).thenReturn(org.apache.spark.status.api.v1.StageStatus.COMPLETE)
    when(stageData.executorRunTime).thenReturn(1000L)
    when(stageData.inputBytes).thenReturn(1000L)
    when(stageData.outputBytes).thenReturn(1000L)
    when(stageData.shuffleReadBytes).thenReturn(1000L)
    when(stageData.shuffleWriteBytes).thenReturn(1000L)

    val jobData = mock[org.apache.spark.status.api.v1.JobData]
    when(jobData.jobId).thenReturn(1)
    when(jobData.status).thenReturn(org.apache.spark.JobExecutionStatus.SUCCEEDED)
    when(jobData.stageIds).thenReturn(Seq(1))
    when(jobData.numTasks).thenReturn(1)
    when(jobData.submissionTime).thenReturn(Some(new java.util.Date(0)))
    when(jobData.completionTime).thenReturn(Some(new java.util.Date(0)))

    val executorSummary = mock[org.apache.spark.status.api.v1.ExecutorSummary]
    when(executorSummary.id).thenReturn("1")
    when(executorSummary.hostPort).thenReturn("localhost:12345")
    when(executorSummary.isActive).thenReturn(true)

    val appInfo = mock[ApplicationInfo]
    when(appInfo.id).thenReturn("app-id")
    when(appInfo.name).thenReturn("app-name")

    val appData = SparkApplicationData("app-id-1", Map(), appInfo, Seq(jobData), Seq(stageData), Seq(executorSummary), Map())

    val result = HtmlReportAnalyzer.analysis(appData)
    assert(result.name == "HTML Report")
    assert(result.rows.nonEmpty)
    assert(result.rows.head.head.contains("<h1>Spark Insight Report for app-id-1</h1>"))
  }
}
