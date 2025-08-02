package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.StageData

/**
 * An analyzer that compares two Spark applications at the stage level.
 */
object StageLevelDiffAnalyzer extends Analyzer {

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val stages1 = data1.stageData.map(s => s.stageId -> s).toMap
    val stages2 = data2.stageData.map(s => s.stageId -> s).toMap

    val commonStageIds = stages1.keySet.intersect(stages2.keySet)

    val rows = commonStageIds.map { id =>
      val stage1 = stages1(id)
      val stage2 = stages2(id)

      val durationDiff = stage2.executorRunTime - stage1.executorRunTime
      val inputDiff = stage2.inputBytes - stage1.inputBytes
      val outputDiff = stage2.outputBytes - stage1.outputBytes
      val shuffleReadDiff = stage2.shuffleReadBytes - stage1.shuffleReadBytes
      val shuffleWriteDiff = stage2.shuffleWriteBytes - stage1.shuffleWriteBytes

      (
        durationDiff,
        Seq(
          id.toString,
          stage1.name,
          s"${durationDiff}ms",
          s"${inputDiff}B",
          s"${outputDiff}B",
          s"${shuffleReadDiff}B",
          s"${shuffleWriteDiff}B"
        )
      )
    }.toSeq.sortBy(_._1.abs).reverse.map(_._2)

    AnalysisResult(
      "Stage Level Performance Diff",
      Seq("Stage ID", "Name", "Duration Diff", "Input Diff", "Output Diff", "Shuffle Read Diff", "Shuffle Write Diff"),
      rows
    )
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("StageLevelDiffAnalyzer requires two Spark applications to compare.")
  }
}
