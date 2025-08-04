package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.{ApplicationInfo, StageData}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class StageLevelDiffAnalyzerSuite extends AnyFunSuite with MockitoSugar {

  test("StageLevelDiffAnalyzer should generate a diff report") {
    val stageData1 = mock[StageData]
    when(stageData1.stageId).thenReturn(1)
    when(stageData1.name).thenReturn("stage1")
    when(stageData1.executorRunTime).thenReturn(60000L)
    when(stageData1.inputBytes).thenReturn(1024L * 1024L)
    when(stageData1.outputBytes).thenReturn(1024L * 1024L)
    when(stageData1.shuffleReadBytes).thenReturn(1024L * 1024L)
    when(stageData1.shuffleWriteBytes).thenReturn(1024L * 1024L)

    val stageData2 = mock[StageData]
    when(stageData2.stageId).thenReturn(1)
    when(stageData2.name).thenReturn("stage1")
    when(stageData2.executorRunTime).thenReturn(120000L)
    when(stageData2.inputBytes).thenReturn(2 * 1024L * 1024L)
    when(stageData2.outputBytes).thenReturn(2 * 1024L * 1024L)
    when(stageData2.shuffleReadBytes).thenReturn(2 * 1024L * 1024L)
    when(stageData2.shuffleWriteBytes).thenReturn(2 * 1024L * 1024L)

    val appInfo1 = mock[ApplicationInfo]
    when(appInfo1.id).thenReturn("app-id-1")
    val appInfo2 = mock[ApplicationInfo]
    when(appInfo2.id).thenReturn("app-id-2")

    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo1, Seq(), Seq(stageData1), Seq(), Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo2, Seq(), Seq(stageData2), Seq(), Map())

    val result = StageLevelDiffAnalyzer.analysis(appData1, appData2)
    assert(result.name == s"Stage Level Diff Report for ${appData1.appInfo.id} and ${appData2.appInfo.id}")
    assert(result.header == Seq("Stage ID", "Name", "Duration Diff", "Input Diff", "Output Diff", "Shuffle Read Diff", "Shuffle Write Diff"))
    assert(result.rows.nonEmpty)
    assert(result.rows.head.head == "1")
    assert(result.rows.head(2) == "1min (100.00%)")
    assert(result.rows.head(3) == "1MB (100.00%)")
  }
}