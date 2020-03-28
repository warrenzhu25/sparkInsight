package com.microsoft.spark.insight.fetcher

trait Fetcher {
  def fetchData(trackingUri: String): SparkApplicationData
}
