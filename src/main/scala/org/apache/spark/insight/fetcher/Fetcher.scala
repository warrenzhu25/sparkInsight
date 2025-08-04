
package org.apache.spark.insight.fetcher

/**
 * Trait for fetching Spark application data.
 */
trait Fetcher {
  def fetchData(trackingUri: String): SparkApplicationData
  def getRecentApplications(historyServerUri: String, appName: Option[String], limit: Int = 2): Seq[SparkApplicationData]
}
