package com.microsoft.spark.insight

import java.util.concurrent.Callable

import com.microsoft.spark.insight.fetcher.SparkFetcher
import com.microsoft.spark.insight.heuristics.{ConfigurationHeuristic, ExecutorGcHeuristic, ExecutorsHeuristic, JobsHeuristic, StagesHeuristic}
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

@Command(name = "SparkInisght", version = Array("v0.1"),
  mixinStandardHelpOptions = true, // add --help and --version options
  description = Array("SparkInsight - Auto tuning and failure analysic"))
class SparkInsight extends Callable[Int] {

  private val heuristic = Seq(
    ConfigurationHeuristic,
    ExecutorGcHeuristic,
    ExecutorsHeuristic,
    JobsHeuristic,
    StagesHeuristic
  )

  @Option(names = Array("-u", "--count"), paramLabel = "URL",
    description = Array("Spark job tracking url"))
  private var trackingUrl: String = ""

  def call(): Int = {
    val appData = SparkFetcher.fetchData(trackingUrl)
    heuristic.map(_.apply(appData)).foreach(print)
    0
  }
}

object SparkInsight {
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new SparkInsight()).execute(args: _*))
  }
}
