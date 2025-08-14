package org.apache.spark.insight

import org.apache.spark.insight.analyzer._
import org.apache.spark.insight.fetcher.{Fetcher, SparkFetcher}
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.util.concurrent.Callable

/**
 * Main class for the Spark Insight application.
 */
@Command(name = "spark-insight-cli", version = Array("v0.1"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("SparkInsight - Auto tuning and failure analysis"),
  subcommands = Array(classOf[RunCommand]))
class SparkInsightCli {}

@Command(name = "run", description = Array("Run analysis on Spark applications"))
class RunCommand extends Callable[Int] {

  private val analyzers = Seq(
    AutoScalingAnalyzer,
    AppSummaryAnalyzer,
    ExecutorAnalyzer,
    ShuffleSkewAnalyzer,
    FailedTaskAnalyzer,
    ShuffleWaitAnalyzer
  )

  @Option(names = Array("-u1", "--url1"), paramLabel = "URL1",
    description = Array("Spark app tracking url 1"))
  private var trackingUrl1: String = _

  @Option(names = Array("-u2", "--url2"), paramLabel = "URL2",
    description = Array("Spark app tracking url 2"))
  private var trackingUrl2: String = _

  @Option(names = Array("-n", "--app-name"), paramLabel = "APP_NAME",
    description = Array("Spark application name"))
  private var appName: String = _

  var fetcher: Fetcher = SparkFetcher

  private def getFullUrl(urlOrAppId: String): String = {
    if (urlOrAppId.startsWith("http")) {
      urlOrAppId
    } else {
      s"http://localhost:18080/history/$urlOrAppId"
    }
  }

  def call(): Int = {
    if (appName != null) {
      val apps = fetcher.getRecentApplications("http://localhost:18080", Some(appName))
      if (apps.size < 2) {
        println("Could not find two recent applications with that name.")
        return 1
      }
      val appData1 = apps(0)
      val appData2 = apps(1)
      println(s"Diffing applications: ${appData1.appId} and ${appData2.appId}")
      AppDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      StageLevelDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      ConfigDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      ExecutorAnalyzer.analysis(appData1).toCliOutput
      ExecutorAnalyzer.analysis(appData2).toCliOutput
    } else if (trackingUrl2 == null) {
      val appData = fetcher.fetchData(getFullUrl(trackingUrl1))
      analyzers.map(_.analysis(appData)).foreach(_.toCliOutput)
    } else {
      val appData1 = fetcher.fetchData(getFullUrl(trackingUrl1))
      val appData2 = fetcher.fetchData(getFullUrl(trackingUrl2))
      AppDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      StageLevelDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      ConfigDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      ExecutorAnalyzer.analysis(appData1).toCliOutput
      ExecutorAnalyzer.analysis(appData2).toCliOutput
    }
    0
  }
}



/**
 * Companion object for the Spark Insight application.
 */
object SparkInsightCli {
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new SparkInsightCli()).execute(args: _*))
  }
}