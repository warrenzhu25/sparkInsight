package org.apache.spark.insight.fetcher

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

import scala.concurrent.duration.{Duration, HOURS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Fetcher {
  def fetchData(trackingUri: String): SparkApplicationData
}

object SparkFetcher extends Fetcher {

  import ExecutionContext.Implicits.global

  private[fetcher] lazy val hadoopConfiguration: Configuration = new Configuration()
  private[fetcher] lazy val sparkRestClient: SparkRestClient = new SparkRestClient()
  private val logger: Logger = Logger.getLogger(SparkFetcher.getClass)
  private val DEFAULT_TIMEOUT = Duration(1, HOURS)

  override def fetchData(trackingUrl: String): SparkApplicationData = {
    doFetchData(trackingUrl) match {
      case Success(data) => data
      case Failure(e) =>
        logger.error(e)
        throw e
    }
  }

  private def doFetchData(trackingUrl: String): Try[SparkApplicationData] = {
    Try {
      Await.result(doFetchSparkApplicationData(trackingUrl), DEFAULT_TIMEOUT)
    }.transform(
      data => {
        logger.info(s"Succeeded fetching data for ${trackingUrl}")
        Success(data)
      },
      e => {
        logger.warn(s"Failed fetching data for ${trackingUrl}.", e)
        Failure(e)
      })
  }

  private def doFetchSparkApplicationData(trackingUrl: String): Future[SparkApplicationData] = {
    doFetchDataUsingRestClients(trackingUrl)
  }

  private def doFetchDataUsingRestClients(trackingUrl: String): Future[SparkApplicationData] =
    Future {
      Await.result(sparkRestClient.fetchData(trackingUrl), DEFAULT_TIMEOUT)
    }
}
