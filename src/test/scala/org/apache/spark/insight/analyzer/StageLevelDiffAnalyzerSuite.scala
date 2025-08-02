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
    when(stageData1.executorRunTime).thenReturn(1000L)
    when(stageData1.inputBytes).thenReturn(1000L)
    when(stageData1.outputBytes).thenReturn(1000L)
    when(stageData1.shuffleReadBytes).thenReturn(1000L)
    when(stageData1.shuffleWriteBytes).thenReturn(1000L)

    val stageData2 = mock[StageData]
    when(stageData2.stageId).thenReturn(1)
    when(stageData2.name).thenReturn("stage1")
    when(stageData2.executorRunTime).thenReturn(2000L)
    when(stageData2.inputBytes).thenReturn(2000L)
    when(stageData2.outputBytes).thenReturn(2000L)
    when(stageData2.shuffleReadBytes).thenReturn(2000L)
    when(stageData2.shuffleWriteBytes).thenReturn(2000L)

    val appInfo = mock[ApplicationInfo]
    when(appInfo.id).thenReturn("app-id")
    when(appInfo.name).thenReturn("app-name")

    val appData1 = SparkApplicationData("app-id-1", Map(), appInfo, Seq(), Seq(stageData1), Seq(), Map())
    val appData2 = SparkApplicationData("app-id-2", Map(), appInfo, Seq(), Seq(stageData2), Seq(), Map())

    val result = StageLevelDiffAnalyzer.analysis(appData1, appData2)
    assert(result.name == "Stage Level Performance Diff")
    assert(result.header == Seq("Stage ID", "Name", "Duration Diff", "Input Diff", "Output Diff", "Shuffle Read Diff", "Shuffle Write Diff"))
    assert(result.rows.nonEmpty)
  }
}
