package org.apache.spark.insight.mcp.tools

import cats.effect.IO
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.analyzer._
import org.apache.spark.insight.fetcher.{SparkFetcher, SparkRestClient}

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

  private val restClient = new SparkRestClient()

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
    ),
    Tool(
      name = "list_applications",
      description = "List all applications available in the Spark History Server",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "history_server_url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark History Server URL (e.g., http://localhost:18080)"),
            "default" -> Json.fromString("http://localhost:18080")
          )
        ),
        "required" -> Json.arr()
      )
    ),
    Tool(
      name = "get_executors",
      description = "Get detailed executor information for a Spark application",
      inputSchema = Json.obj(
        "type" -> Json.fromString("object"),
        "properties" -> Json.obj(
          "url" -> Json.obj(
            "type" -> Json.fromString("string"),
            "description" -> Json.fromString("Spark application tracking URL")
          ),
          "active_only" -> Json.obj(
            "type" -> Json.fromString("boolean"),
            "description" -> Json.fromString("Return only active executors (default: false)"),
            "default" -> Json.fromBoolean(false)
          )
        ),
        "required" -> Json.arr(Json.fromString("url"))
      )
    ),
    Tool(
      name = "get_application_info",
      description = "Get detailed application metadata and configuration",
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
      name = "get_jobs_info",
      description = "Get detailed job information for a Spark application",
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
      name = "get_stages_info",
      description = "Get detailed stage information and metrics for a Spark application",
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
      case "list_applications" => listApplications(arguments)
      case "get_executors" => getExecutors(arguments)
      case "get_application_info" => getApplicationInfo(arguments)
      case "get_jobs_info" => getJobsInfo(arguments)
      case "get_stages_info" => getStagesInfo(arguments)
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

  private def listApplications(arguments: Option[Json]): IO[CallToolResult] = {
    val historyServerUrl = arguments.flatMap { args =>
      args.hcursor.get[String]("history_server_url").toOption
    }.getOrElse("http://localhost:18080")

    IO {
      try {
        val applications = restClient.listApplications(historyServerUrl)
        val appList = applications.map { app =>
          s"- **${app.name}** (${app.id})\n" +
          s"  - Status: ${if (app.attempts.head.completed) "Completed" else "Running"}\n" +
          s"  - Start Time: ${app.attempts.head.startTime}\n" +
          s"  - Duration: ${s"${app.attempts.head.duration / 1000}s"}\n" +
          s"  - URL: ${historyServerUrl}/history/${app.id}"
        }.mkString("\n\n")

        val summary = s"""## Available Spark Applications (${applications.length} total)

${appList}

**Usage:** Use any of the application URLs above with other analysis tools."""

        CallToolResult(
          content = List(TextContent(text = summary))
        )
      } catch {
        case e: Exception =>
          CallToolResult(
            content = List(TextContent(text = s"Error listing applications: ${e.getMessage}")),
            isError = Some(true)
          )
      }
    }
  }

  private def getExecutors(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        (for {
          url <- args.hcursor.get[String]("url")
          activeOnly = args.hcursor.get[Boolean]("active_only").toOption.getOrElse(false)
        } yield (url, activeOnly)) match {
          case Right((url, activeOnly)) =>
            IO {
              try {
                val executors = if (activeOnly) {
                  restClient.listActiveExecutors(url)
                } else {
                  restClient.listExecutors(url)
                }

                val executorList = executors.map { exec =>
                  s"- **Executor ${exec.id}**\n" +
                  s"  - Host: ${exec.hostPort}\n" +
                  s"  - Status: ${if (exec.isActive) "Active" else "Inactive"}\n" +
                  s"  - Cores: ${exec.totalCores}\n" +
                  s"  - Memory: ${exec.maxMemory / (1024 * 1024)}MB\n" +
                  s"  - Tasks: ${exec.totalTasks} total, ${exec.activeTasks} active, ${exec.completedTasks} completed, ${exec.failedTasks} failed\n" +
                  s"  - GC Time: ${exec.totalGCTime}ms\n" +
                  s"  - Input: ${exec.totalInputBytes / (1024 * 1024)}MB\n" +
                  s"  - Shuffle Read: ${exec.totalShuffleRead / (1024 * 1024)}MB\n" +
                  s"  - Shuffle Write: ${exec.totalShuffleWrite / (1024 * 1024)}MB"
                }.mkString("\n\n")

                val summary = s"""## Executor Information (${executors.length} ${if (activeOnly) "active " else ""}executors)

${executorList}

**Summary:**
- Total Cores: ${executors.map(_.totalCores).sum}
- Total Memory: ${executors.map(_.maxMemory).sum / (1024 * 1024)}MB
- Active Executors: ${executors.count(_.isActive)}
- Total Tasks: ${executors.map(_.totalTasks).sum}"""

                CallToolResult(
                  content = List(TextContent(text = summary))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error getting executors: ${e.getMessage}")),
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

  private def getApplicationInfo(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val app = appData.appInfo
                val attempt = app.attempts.head

                val sparkProps = appData.appConf.toSeq.sortBy(_._1).map { case (key, value) =>
                  s"  - **${key}**: ${value}"
                }.mkString("\n")

                val summary = s"""## Application Information

**Basic Details:**
- **Name**: ${app.name}
- **Application ID**: ${app.id}
- **User**: ${attempt.sparkUser}
- **Status**: ${if (attempt.completed) "Completed" else "Running"}
- **Start Time**: ${attempt.startTime}
- **End Time**: ${attempt.endTime}
- **Duration**: ${s"${attempt.duration / 1000}s"}

**Spark Configuration:**
${sparkProps}

**Attempt Details:**
- **Attempt ID**: ${attempt.attemptId.getOrElse("N/A")}
- **Last Updated**: ${attempt.lastUpdated}"""

                CallToolResult(
                  content = List(TextContent(text = summary))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error getting application info: ${e.getMessage}")),
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

  private def getJobsInfo(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val jobs = appData.jobData

                val jobList = jobs.map { job =>
                  s"- **Job ${job.jobId}**\n" +
                  s"  - Name: ${job.name}\n" +
                  s"  - Status: ${job.status}\n" +
                  s"  - Stages: ${job.stageIds.length}\n" +
                  s"  - Tasks: ${job.numTasks} total, ${job.numActiveTasks} active, ${job.numCompletedTasks} completed, ${job.numFailedTasks} failed\n" +
                  s"  - Start Time: ${job.submissionTime.getOrElse("N/A")}\n" +
                  s"  - Completion Time: ${job.completionTime.getOrElse("N/A")}"
                }.mkString("\n\n")

                val summary = s"""## Job Information (${jobs.length} jobs)

${jobList}

**Summary:**
- Total Jobs: ${jobs.length}
- Succeeded: ${jobs.count(_.status == "SUCCEEDED")}
- Failed: ${jobs.count(_.status == "FAILED")}
- Running: ${jobs.count(_.status == "RUNNING")}
- Total Tasks: ${jobs.map(_.numTasks).sum}"""

                CallToolResult(
                  content = List(TextContent(text = summary))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error getting jobs info: ${e.getMessage}")),
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

  private def getStagesInfo(arguments: Option[Json]): IO[CallToolResult] = {
    arguments match {
      case Some(args) =>
        args.hcursor.get[String]("url") match {
          case Right(url) =>
            IO {
              try {
                val appData = SparkFetcher.fetchData(url)
                val stages = appData.stageData

                val stageList = stages.map { stage =>
                  val inputMB = stage.inputBytes / (1024 * 1024)
                  val outputMB = stage.outputBytes / (1024 * 1024)
                  val shuffleReadMB = stage.shuffleReadBytes / (1024 * 1024)
                  val shuffleWriteMB = stage.shuffleWriteBytes / (1024 * 1024)

                  s"- **Stage ${stage.stageId}.${stage.attemptId}**\n" +
                  s"  - Name: ${stage.name}\n" +
                  s"  - Status: ${stage.status}\n" +
                  s"  - Tasks: ${stage.numTasks} total, ${stage.numActiveTasks} active, ${stage.numCompleteTasks} completed, ${stage.numFailedTasks} failed\n" +
                  s"  - Duration: ${stage.executorRunTime / 1000}s\n" +
                  s"  - Input: ${inputMB}MB (${stage.inputRecords} records)\n" +
                  s"  - Output: ${outputMB}MB (${stage.outputRecords} records)\n" +
                  s"  - Shuffle Read: ${shuffleReadMB}MB (${stage.shuffleReadRecords} records)\n" +
                  s"  - Shuffle Write: ${shuffleWriteMB}MB (${stage.shuffleWriteRecords} records)\n" +
                  s"  - Memory Spill: ${stage.memoryBytesSpilled / (1024 * 1024)}MB\n" +
                  s"  - Disk Spill: ${stage.diskBytesSpilled / (1024 * 1024)}MB"
                }.mkString("\n\n")

                val summary = s"""## Stage Information (${stages.length} stages)

${stageList}

**Summary:**
- Total Stages: ${stages.length}
- Completed: ${stages.count(_.status == "COMPLETE")}
- Failed: ${stages.count(_.status == "FAILED")}
- Running: ${stages.count(_.status == "RUNNING")}
- Total Tasks: ${stages.map(_.numTasks).sum}
- Total Input: ${stages.map(_.inputBytes).sum / (1024 * 1024)}MB
- Total Output: ${stages.map(_.outputBytes).sum / (1024 * 1024)}MB
- Total Shuffle Read: ${stages.map(_.shuffleReadBytes).sum / (1024 * 1024)}MB
- Total Shuffle Write: ${stages.map(_.shuffleWriteBytes).sum / (1024 * 1024)}MB"""

                CallToolResult(
                  content = List(TextContent(text = summary))
                )
              } catch {
                case e: Exception =>
                  CallToolResult(
                    content = List(TextContent(text = s"Error getting stages info: ${e.getMessage}")),
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