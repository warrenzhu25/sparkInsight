package org.apache.spark.insight

import org.apache.spark.insight.analyzer.{AppSummaryAnalyzer, InitialExecutorAnalyzer}
import org.apache.spark.insight.fetcher.SparkFetcher
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.util.concurrent.Callable

@Command(name = "SparkInsight", version = Array("v0.1"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("SparkInsight - Auto tuning and failure analysis"))
class SparkInsight extends Callable[Int] {

  private val analyzers = Seq(
    InitialExecutorAnalyzer,
    AppSummaryAnalyzer,
  )

  @Option(names = Array("-u", "--url"), paramLabel = "URL",
    description = Array("Spark app tracking url"))
  private var trackingUrl: String = "http://localhost:18080/history/app-20231129012909-0000"

  def call(): Int = {
    val appData = SparkFetcher.fetchData(trackingUrl)
    analyzers.map(_.analysis(appData)).foreach(_.toCliOutput)
    0
  }
}

object SparkInsight {
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new SparkInsight()).execute(args: _*))
  }
}
