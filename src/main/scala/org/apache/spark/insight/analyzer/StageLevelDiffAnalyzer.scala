package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.StageData

import java.util.concurrent.TimeUnit

/**
 * An analyzer that compares two Spark applications at the stage level.
 */
object StageLevelDiffAnalyzer extends Analyzer {

  override def analysis(data1: SparkApplicationData, data2: SparkApplicationData): AnalysisResult = {
    val stages1 = data1.stageData.map(s => s.stageId -> s).toMap
    val stages2 = data2.stageData.map(s => s.stageId -> s).toMap

    val commonStageIds = stages1.keySet.intersect(stages2.keySet)

    val rows = commonStageIds.flatMap { id =>
      val stage1 = stages1(id)
      val stage2 = stages2(id)

      val durationDiff = stage2.executorRunTime - stage1.executorRunTime
      val inputDiff = stage2.inputBytes - stage1.inputBytes
      val outputDiff = stage2.outputBytes - stage1.outputBytes
      val shuffleReadDiff = stage2.shuffleReadBytes - stage1.shuffleReadBytes
      val shuffleWriteDiff = stage2.shuffleWriteBytes - stage1.shuffleWriteBytes

      if (durationDiff == 0 && inputDiff == 0 && outputDiff == 0 && shuffleReadDiff == 0 && shuffleWriteDiff == 0) {
        None
      } else {
        val durationDiffPercentage = if (stage1.executorRunTime == 0) "N/A" else f"${(durationDiff * 100.0 / stage1.executorRunTime)}%.2f%%"
        val inputDiffPercentage = if (stage1.inputBytes == 0) "N/A" else f"${(inputDiff * 100.0 / stage1.inputBytes)}%.2f%%"
        val outputDiffPercentage = if (stage1.outputBytes == 0) "N/A" else f"${(outputDiff * 100.0 / stage1.outputBytes)}%.2f%%"
        val shuffleReadDiffPercentage = if (stage1.shuffleReadBytes == 0) "N/A" else f"${(shuffleReadDiff * 100.0 / stage1.shuffleReadBytes)}%.2f%%"
        val shuffleWriteDiffPercentage = if (stage1.shuffleWriteBytes == 0) "N/A" else f"${(shuffleWriteDiff * 100.0 / stage1.shuffleWriteBytes)}%.2f%%"

        Some((
          durationDiff,
          Seq(
            id.toString,
            stage1.name,
            s"${TimeUnit.MILLISECONDS.toMinutes(durationDiff)}min ($durationDiffPercentage)",
            s"${inputDiff / (1024 * 1024)}MB ($inputDiffPercentage)",
            s"${outputDiff / (1024 * 1024)}MB ($outputDiffPercentage)",
            s"${shuffleReadDiff / (1024 * 1024)}MB ($shuffleReadDiffPercentage)",
            s"${shuffleWriteDiff / (1024 * 1024)}MB ($shuffleWriteDiffPercentage)"
          )
        ))
      }
    }.toSeq.sortBy(_._1.abs).reverse.take(5).map(_._2)

    AnalysisResult(
      s"Stage Level Diff Report for ${data1.appInfo.id} and ${data2.appInfo.id}",
      Seq("Stage ID", "Name", "Duration Diff", "Input Diff", "Output Diff", "Shuffle Read Diff", "Shuffle Write Diff"),
      rows,
      "Compares the performance of common stages between two Spark applications."
    )
  }

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    throw new UnsupportedOperationException("StageLevelDiffAnalyzer requires two Spark applications to compare.")
  }
}