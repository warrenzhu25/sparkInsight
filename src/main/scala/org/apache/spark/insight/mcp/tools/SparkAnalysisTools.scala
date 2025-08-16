package org.apache.spark.insight.mcp.tools

import cats.effect.IO
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.analyzer._
import org.apache.spark.insight.fetcher.SparkFetcher

/**
 * SparkInsight MCP Tools implementation
 * Provides tools for AI assistants to analyze Spark applications
 */
class SparkAnalysisTools {

  private val analyzers = Map(
    "app_summary" -> AppSummaryAnalyzer,
    "auto_scaling" -> AutoScalingAnalyzer,
    "shuffle_skew" -> ShuffleSkewAnalyzer,
    "executor" -> ExecutorAnalyzer
  )

  def getAvailableTools(): List[Tool] = List(
    Tool(
      name = "analyze_spark_app",
      description = "Analyze a single Spark application for performance insights and recommendations",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark application tracking URL (e.g., http://localhost:18080/history/app-20240228220418-0000)")
          ),
          "analyzers" -> Json.obj(
            "type" -> Json.fromString("array"),
            "items" -> Json.obj("type" -> Json.fromString("string")),
            "description" -> Json.fromString("Optional list of specific analyzers to run: app_summary, auto_scaling, shuffle_skew, executor"),
            "default" -> Json.arr(Json.fromString("app_summary"), Json.fromString("auto_scaling"))
          )
        ),
        "required" -> Json.arr(Json.fromString("url"))
      )
    ),
    Tool(
      name = "compare_spark_apps",
      description = "Compare two Spark applications to identify performance differences and regressions",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url1" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("First Spark application tracking URL")
          ),
          "url2" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Second Spark application tracking URL")
          )
        ),
        "required" -> Json.arr(Json.fromString("url1"), Json.fromString("url2"))
      )
    ),
    Tool(
      name = "get_app_summary",
      description = "Get a quick summary of Spark application metrics and basic statistics",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark application tracking URL")
          )
        ),
        "required" -> Json.arr(Json.fromString("url"))
      )
    ),
    Tool(
      name = "analyze_shuffle_skew",
      description = "Analyze shuffle operations for data skew and performance bottlenecks",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark application tracking URL")
          )
        ),
        "required" -> Json.arr(Json.fromString("url"))
      )
    ),
    Tool(
      name = "analyze_executor_usage",
      description = "Analyze executor resource utilization and identify optimization opportunities",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark application tracking URL")
          )
        ),
        "required" -> Json.arr(Json.fromString("url"))
      )
    )
  )

  def callTool(name: String, arguments: Option[Json]): IO[CallToolResult] = {
    name match {
      case "analyze_spark_app" => analyzeSparkApp(arguments)
      case "compare_spark_apps" => compareSparkApps(arguments)
      case "get_app_summary" => getAppSummary(arguments)
      case "analyze_shuffle_skew" => analyzeShuffleSkew(arguments)
      case "analyze_executor_usage" => analyzeExecutorUsage(arguments)
      case _ => IO.pure(CallToolResult(
        content = List(TextContent(text = s"Unknown tool: $name")),
        isError = Some(true)
      ))
    }
  }

  private def analyzeSparkApp(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        (for {
          url <- args.hcursor.get[String]("url")
          analyzersOpt = args.hcursor.get[List[String]]("analyzers").toOption.getOrElse(List("app_summary", "auto_scaling"))
        } yield (url, analyzersOpt)) match {
          case Right((url, analyzerNames)) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val results = analyzerNames.flatMap { name =>
                  analyzers.get(name).map { analyzer =>
                    val result = analyzer.analysis(appData)
                    s"## ${analyzer.getClass.getSimpleName} Analysis\\n${formatAnalysisResult(result)}"
                  }
                }
                CallToolResult(
                  content = List(TextContent(text = results.mkString("\\n\\n")))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error analyzing Spark application: ${e.getMessage}")),
                    isError = Some(true)
                  )
              }
            }
          case Left(error) =>
            IO.pure(CallToolResult(
              content = List(TextContent(text = s"Invalid arguments: $error")),
              isError = Some(true)
            ))
        }
      case None =>
        IO.pure(CallToolResult(
          content = List(TextContent(text = "Missing arguments")),
          isError = Some(true)
        ))
    }
  }

  private def compareSparkApps(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        (for {
          url1 <- args.hcursor.get[String]("url1")
          url2 <- args.hcursor.get[String]("url2")
        } yield (url1, url2)) match {
          case Right((url1, url2)) =>
            IO {
              try {
                val appData1 = SparkFetcher.fetchData(url1)
                val appData2 = SparkFetcher.fetchData(url2)
                val diffResult = AppDiffAnalyzer.analysis(appData1, appData2)
                val stageDiffResult = StageLevelDiffAnalyzer.analysis(appData1, appData2)
                
                val report = s"""## Application Comparison Report
                  |
                  |### Application Diff Analysis
                  |${formatAnalysisResult(diffResult)}
                  |
                  |### Stage Level Diff Analysis  
                  |${formatAnalysisResult(stageDiffResult)}
                  |""".stripMargin
                
                CallToolResult(
                  content = List(TextContent(text = report))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error comparing Spark applications: ${e.getMessage}")),
                    isError = Some(true)
                  )
              }
            }
          case Left(error) =>
            IO.pure(CallToolResult(
              content = List(TextContent(text = s"Invalid arguments: $error")),
              isError = Some(true)
            ))
        }
      case None =>
        IO.pure(CallToolResult(
          content = List(TextContent(text = "Missing arguments")),
          isError = Some(true)
        ))
    }
  }

  private def getAppSummary(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val summaryResult = AppSummaryAnalyzer.analysis(appData)
                CallToolResult(
                  content = List(TextContent(text = formatAnalysisResult(summaryResult)))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error getting app summary: ${e.getMessage}")),
                    isError = Some(true)
                  )
              }
            }
          case Left(error) =>
            IO.pure(CallToolResult(
              content = List(TextContent(text = s"Invalid arguments: $error")),
              isError = Some(true)
            ))
        }
      case None =>
        IO.pure(CallToolResult(
          content = List(TextContent(text = "Missing arguments")),
          isError = Some(true)
        ))
    }
  }

  private def analyzeShuffleSkew(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val skewResult = ShuffleSkewAnalyzer.analysis(appData)
                CallToolResult(
                  content = List(TextContent(text = formatAnalysisResult(skewResult)))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error analyzing shuffle skew: ${e.getMessage}")),
                    isError = Some(true)
                  )
              }
            }
          case Left(error) =>
            IO.pure(CallToolResult(
              content = List(TextContent(text = s"Invalid arguments: $error")),
              isError = Some(true)
            ))
        }
      case None =>
        IO.pure(CallToolResult(
          content = List(TextContent(text = "Missing arguments")),
          isError = Some(true)
        ))
    }
  }

  private def analyzeExecutorUsage(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val executorResult = ExecutorAnalyzer.analysis(appData)
                CallToolResult(
                  content = List(TextContent(text = formatAnalysisResult(executorResult)))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error analyzing executor usage: ${e.getMessage}")),
                    isError = Some(true)
                  )
              }
            }
          case Left(error) =>
            IO.pure(CallToolResult(
              content = List(TextContent(text = s"Invalid arguments: $error")),
              isError = Some(true)
            ))
        }
      case None =>
        IO.pure(CallToolResult(
          content = List(TextContent(text = "Missing arguments")),
          isError = Some(true)
        ))
    }
  }

  private def formatAnalysisResult(result: Any): String = {
    result.toString
  }
}