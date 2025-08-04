package org.apache.spark.insight.fetcher

import org.apache.spark.insight.util.SparkJsonUtils._
import org.apache.log4j.Logger
import org.apache.spark.status.api.v1._
import org.glassfish.jersey.client.ClientProperties

import java.io.BufferedInputStream
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream
import javax.ws.rs.client.{Client, ClientBuilder, WebTarget}
import javax.ws.rs.core.MediaType
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.control.NonFatal

/**
 * A REST client for fetching Spark application data from the Spark History Server.
 */
class SparkRestClient {

  import SparkRestClient._

  private val logger: Logger = Logger.getLogger(classOf[SparkRestClient])

  private val client: Client = ClientBuilder.newClient()

  def listApplications(historyServerUri: String): Seq[ApplicationInfo] = {
    val apiTarget = getApiTarget(historyServerUri)
    val target = apiTarget.path("applications")
    get(target, fromJson[Seq[ApplicationInfo]])
  }

  def fetchData(trackingUrl: String)(
    implicit ec: ExecutionContext
  ): Future[SparkApplicationData] = {
    val (historyServerUri, appId) = spilt(trackingUrl)

    Future {
      val localData = readDataLocally(appId)

      if (localData.nonEmpty) {
        localData.get
      } else {
        val (applicationInfo, env, attemptTarget) = getApplicationMetaData(appId, historyServerUri)

        val futureJobData = Future {
          getJobData(attemptTarget)
        }
        val futureStageData = Future {
          getStageData(attemptTarget)
        }
        val futureExecutorSummaries = Future {
          getExecutorSummaries(attemptTarget)
        }
        val futureTasks = Future {
          getTasksOfFailedStages(attemptTarget)
        }

        val appData = SparkApplicationData(
          appId,
          env.sparkProperties.map(p => p._1 -> p._2).toMap,
          applicationInfo,
          Await.result(futureJobData, DEFAULT_TIMEOUT),
          Await.result(futureStageData, DEFAULT_TIMEOUT),
          Await.result(futureExecutorSummaries, DEFAULT_TIMEOUT),
          Await.result(futureTasks, DEFAULT_TIMEOUT)
        )

        writeDataLocally(appData)
        appData
      }
    }
  }

  private def readDataLocally(appId: String): Option[SparkApplicationData] = {
    val path = s"""D:\\SparkInsight\\$appId.json"""

    if (!Files.exists(Paths.get(path))) {
      return None
    }

    val bufferedSource = Source.fromFile(path)

    try {
      logger.info(s"Reading data from $path")
      Some(SCALA_OBJECT_MAPPER.readValue(bufferedSource.mkString, classOf[SparkApplicationData]))
    } finally {
      bufferedSource.close()
    }
  }

  private def writeDataLocally(sparkApplicationData: SparkApplicationData) = {
    val userHome = System.getProperty("user.home")
    val file = s"""$userHome/SparkInsight/${sparkApplicationData.appId}.json"""
    val path = Paths.get(file)
    if (!Files.exists(path)) {
      logger.info(s"Writing data to $file")
      Files.createDirectories(path.getParent)
      Files.createFile(path)
      Files.write(path, SCALA_OBJECT_MAPPER.writeValueAsBytes(sparkApplicationData))
    }
  }

  private def spilt(trackingUrl: String): (String, String) = {
    val uri = new URI(trackingUrl)
    val host = s"${uri.getScheme}://${uri.getHost}:${uri.getPort}"
    val path = uri.getPath
    val appId = path.substring(path.lastIndexOf('/') + 1)
    (host, appId)
  }

  private def getApplicationMetaData(appId: String, historyServerUri: String):
  (ApplicationInfo, ApplicationEnvironmentInfo, WebTarget) = {
    val apiTarget = getApiTarget(historyServerUri)
    val appTarget = apiTarget.path(s"applications/${appId}")
    logger.info(s"calling REST API at ${appTarget.getUri}")

    val applicationInfo = getApplicationInfo(appTarget)

    val lastAttemptId = applicationInfo.attempts.maxBy {
      _.startTime
    }.attemptId
    val attemptTarget = lastAttemptId.map(appTarget.path).getOrElse(appTarget)
    val applicationEnvironmentInfo = getEnv(attemptTarget)
    (applicationInfo, applicationEnvironmentInfo, attemptTarget)
  }

  private def getApiTarget(historyServerUri: String): WebTarget =
    client
      .property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
      .property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT)
      .target(historyServerUri)
      .path(API_V1_MOUNT_PATH)

  private def getApplicationInfo(appTarget: WebTarget): ApplicationInfo = {
    get(appTarget, s => SCALA_OBJECT_MAPPER.readValue(s, classOf[ApplicationInfo]))
  }

  private def getEnv(attemptTarget: WebTarget): ApplicationEnvironmentInfo = {
    val target = attemptTarget.path("environment")
    get(target, fromJson[ApplicationEnvironmentInfo])
  }

  private def getJobData(attemptTarget: WebTarget): Seq[JobData] = {
    val target = attemptTarget.path("jobs")
    get(target, fromJson[Seq[JobData]])
  }

  private def getStageData(attemptTarget: WebTarget): Seq[StageData] = {
    val target = attemptTarget.path("stages")
    get(target, fromJson[Seq[StageData]])
  }

  private def getExecutorSummaries(attemptTarget: WebTarget): Seq[ExecutorSummary] = {
    val target = attemptTarget.path("allexecutors")
    get(target, fromJson[Seq[ExecutorSummary]])
  }

  private def getTasksOfFailedStages(attemptTarget: WebTarget): Map[String, Seq[TaskData]] = {
    val stageData = getTasks(attemptTarget)
    stageData.map(s => s"${s.stageId}-${s.attemptId}" -> s.tasks.get.values.toSeq).toMap
  }

  private def getTasks(attemptTarget: WebTarget): Seq[StageData] = {
    val target = attemptTarget.path(s"stages")
      .queryParam("status", "failed")
      .queryParam("details", "true")
    get(target, fromJson[Seq[StageData]])
  }

  private[fetcher] def getApplicationLogs(logTarget: WebTarget): ZipInputStream = {
    try {
      val is = logTarget.request(MediaType.APPLICATION_OCTET_STREAM)
        .get(classOf[java.io.InputStream])
      new ZipInputStream(new BufferedInputStream(is))
    } catch {
      case NonFatal(e) =>
        logger.error(s"error reading logs ${logTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
    }
  }
}

object SparkRestClient {
  val logger: Logger = Logger.getLogger(SparkRestClient.getClass)
  val API_V1_MOUNT_PATH = "api/v1"
  val DEFAULT_TIMEOUT = Duration(1, MINUTES);
  val CONNECTION_TIMEOUT = 60000
  val READ_TIMEOUT = 60000

  def get[T](webTarget: WebTarget, converter: String => T): T =
    try {
      println(s"Fetching URL: ${webTarget.getUri}")
      converter(webTarget.request(MediaType.APPLICATION_JSON).get(classOf[String]))
    } catch {
      case NonFatal(e) =>
        logger.error(s"error fetching ${webTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
    }
}