
package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import org.apache.spark.status.api.v1.StageData

object AppMetrics {

  def getMetrics(sparkAppData: SparkApplicationData): Map[String, Long] = {
    val stageData = sparkAppData.stageData
    stageData
      .map(s => getMetrics(s))
      .reduce(combineSum)
  }

  def getMetrics(stageData: StageData): Map[String, Long] = {
    stageData.getClass.getDeclaredFields
      .filter(f => f.getType == java.lang.Long.TYPE)
      .map(f => {
        f.setAccessible(true)
        f.getName -> f.get(stageData).asInstanceOf[Long]
      })
      .toMap
  }

  def combineSum(left: Map[String, Long], right: Map[String, Long]): Map[String, Long] = {
    left.keySet.union(right.keySet).map(k => k -> (left.getOrElse(k, 0L) + right.getOrElse(k, 0L))).toMap
  }
}
