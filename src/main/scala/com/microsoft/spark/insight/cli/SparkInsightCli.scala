package com.microsoft.spark.insight.cli

import org.apache.spark.insight.analyzer.{AppDiffAnalyzer, AppSummaryAnalyzer, AutoScalingAnalyzer, HtmlReportAnalyzer, StageLevelDiffAnalyzer}
import org.apache.spark.insight.fetcher.SparkFetcher
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.util.concurrent.Callable

/**
 * Main class for the Spark Insight application.
 */
@Command(name = "spark-insight-cli", version = Array("v0.1"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("SparkInsight - Auto tuning and failure analysis"),
  subcommands = Array(classOf[RunCommand], classOf[ServerCommand]))
class SparkInsightCli {}

@Command(name = "run", description = Array("Run analysis on Spark applications"))
class RunCommand extends Callable[Int] {

  private val analyzers = Seq(
    AutoScalingAnalyzer,
    AppSummaryAnalyzer
  )

  @Option(names = Array("-u1", "--url1"), paramLabel = "URL1",
    description = Array("Spark app tracking url 1"),
    required = true)
  private var trackingUrl1: String = _

  @Option(names = Array("-u2", "--url2"), paramLabel = "URL2",
    description = Array("Spark app tracking url 2"))
  private var trackingUrl2: String = _

  @Option(names = Array("--html"), paramLabel = "HTML",
    description = Array("Generate HTML report"))
  private var html: Boolean = false

  def call(): Int = {
    if (html) {
      val appData = SparkFetcher.fetchData(trackingUrl1)
      val htmlReport = HtmlReportAnalyzer.analysis(appData)
      val path = java.nio.file.Paths.get(s"${appData.appId}.html")
      java.nio.file.Files.write(path, htmlReport.rows.head.head.getBytes)
      java.awt.Desktop.getDesktop.browse(path.toUri)
    } else if (trackingUrl2 == null) {
      val appData = SparkFetcher.fetchData(trackingUrl1)
      analyzers.map(_.analysis(appData)).foreach(_.toCliOutput)
    } else {
      val appData1 = SparkFetcher.fetchData(trackingUrl1)
      val appData2 = SparkFetcher.fetchData(trackingUrl2)
      AppDiffAnalyzer.analysis(appData1, appData2).toCliOutput
      StageLevelDiffAnalyzer.analysis(appData1, appData2).toCliOutput
    }
    0
  }
}

@Command(name = "server", description = Array("Start the web server"))
class ServerCommand extends Callable[Int] {
  def call(): Int = {
    com.microsoft.spark.insight.server.WebServer.main(Array())
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
