package org.apache.spark.insight.mcp

import cats.effect.{ExitCode, IO, IOApp}
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.mcp.tools.SparkAnalysisTools
import org.apache.spark.insight.mcp.transport.{HttpTransport, SseTransport}

import scala.io.StdIn

/**
 * MCP Server implementation for SparkInsight
 * Provides Model Context Protocol interface for AI assistants to analyze Spark applications
 * Following MCP Specification 2025-06-18
 */
object McpServer extends IOApp {

  case class Config(
    transport: String = "stdio",
    httpPort: Int = 8080,
    ssePort: Int = 8081,
    debug: Boolean = false,
    help: Boolean = false
  )

  def run(args: List[String]): IO[ExitCode] = {
    parseArgs(args) match {
      case Some(config) if config.help =>
        printHelp().as(ExitCode.Success)
      case Some(config) =>
        runWithConfig(config).as(ExitCode.Success)
      case None =>
        printHelp().as(ExitCode.Error)
    }
  }

  private def parseArgs(args: List[String]): Option[Config] = {
    def parse(args: List[String], config: Config): Option[Config] = args match {
      case Nil => Some(config)
      case "--help" :: _ => Some(config.copy(help = true))
      case "-h" :: _ => Some(config.copy(help = true))
      case "--transport" :: transport :: rest =>
        if (List("stdio", "http", "sse").contains(transport))
          parse(rest, config.copy(transport = transport))
        else None
      case "--http-port" :: port :: rest =>
        try {
          parse(rest, config.copy(httpPort = port.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case "--sse-port" :: port :: rest =>
        try {
          parse(rest, config.copy(ssePort = port.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case "--debug" :: rest =>
        parse(rest, config.copy(debug = true))
      case "-d" :: rest =>
        parse(rest, config.copy(debug = true))
      case unknown :: _ =>
        None
    }
    parse(args, Config())
  }

  private def printHelp(): IO[Unit] = {
    IO.println(
      """SparkInsight MCP Server
        |
        |Usage: java -jar spark-insight-mcp.jar [options]
        |
        |Options:
        |  --transport <mode>    Transport mode: stdio, http, sse (default: stdio)
        |  --http-port <port>   HTTP server port (default: 8080)
        |  --sse-port <port>    SSE server port (default: 8081)
        |  -d, --debug         Enable debug mode with verbose logging
        |  -h, --help          Show this help message
        |
        |Transport Modes:
        |  stdio               Standard input/output (default, for MCP clients like Claude Desktop)
        |  http                HTTP JSON-RPC server
        |  sse                 Server-Sent Events with HTTP POST for bidirectional communication
        |
        |Examples:
        |  java -jar spark-insight-mcp.jar
        |  java -jar spark-insight-mcp.jar --transport http --http-port 9090
        |  java -jar spark-insight-mcp.jar --transport sse --sse-port 9091
        |  java -jar spark-insight-mcp.jar --debug --transport http
        |""".stripMargin
    )
  }

  private def runWithConfig(config: Config): IO[Unit] = {
    val debugInfo = if (config.debug) {
      IO.println(s"Debug mode enabled") *>
        IO.println(s"Config: transport=${config.transport}, httpPort=${config.httpPort}, ssePort=${config.ssePort}")
    } else IO.unit

    debugInfo *> {
      config.transport match {
        case "stdio" => runStdioMode(config.debug)
        case "http" => runHttpMode(config.httpPort, config.debug)
        case "sse" => runSseMode(config.ssePort, config.debug)
      }
    }
  }

  private def runStdioMode(debug: Boolean): IO[Unit] = {
    val debugMsg = if (debug) IO.println("Debug: stdio mode initialization") else IO.unit
    debugMsg *>
      IO.println("Starting MCP server in stdio mode...") *>
      processMessages(debug)
  }

  private def runHttpMode(port: Int, debug: Boolean): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: HTTP mode initialization on port $port") else IO.unit
    debugMsg *>
      IO.println(s"Starting MCP server in HTTP mode on port $port...") *>
      new HttpTransport(port).start().use { server =>
        IO.println(s"HTTP MCP server started on ${server.address}") *>
          IO.println("Available endpoints:") *>
          IO.println(s"  POST http://localhost:$port/mcp - JSON-RPC endpoint") *>
          IO.println(s"  POST http://localhost:$port/mcp/batch - Batch JSON-RPC endpoint") *>
          IO.println(s"  GET  http://localhost:$port/health - Health check") *>
          (if (debug) IO.println(s"Debug: HTTP server running indefinitely") else IO.unit) *>
          IO.never
      }
  }

  private def runSseMode(port: Int, debug: Boolean): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: SSE mode initialization on port $port") else IO.unit
    debugMsg *>
      IO.println(s"Starting MCP server in SSE mode on port $port...") *>
      new SseTransport(port).start().use { server =>
        IO.println(s"SSE MCP server started on ${server.address}") *>
          IO.println("Available endpoints:") *>
          IO.println(s"  GET  http://localhost:$port/mcp/events - SSE connection endpoint") *>
          IO.println(s"  POST http://localhost:$port/mcp/message/{{connectionId}} - Send message endpoint") *>
          IO.println(s"  GET  http://localhost:$port/mcp/connections - List active connections") *>
          IO.println(s"  GET  http://localhost:$port/health - Health check") *>
          (if (debug) IO.println(s"Debug: SSE server running indefinitely") else IO.unit) *>
          IO.never
      }
  }

  private val serverInfo = ServerInfo(
    name = "spark-insight-mcp",
    version = "1.0.0"
  )

  private val serverCapabilities = ServerCapabilities(
    tools = Some(ToolsCapability(listChanged = Some(false)))
  )

  private val analysisTools = new SparkAnalysisTools()


  private def processMessages(debug: Boolean = false): IO[Unit] = {
    for {
      line <- IO(StdIn.readLine())
      _ <- if (line != null) {
        for {
          _ <- if (debug) IO.println(s"Debug: Received message: $line") else IO.unit
          _ <- processMessage(line, debug)
          _ <- processMessages(debug)
        } yield ()
      } else {
        val debugMsg = if (debug) IO.println("Debug: End of input stream, terminating") else IO.unit
        debugMsg
      }
    } yield ()
  }

  private def processMessage(line: String, debug: Boolean = false): IO[Unit] = {
    parser.parse(line) match {
      case Right(json) =>
        val debugMsg = if (debug) IO.println(s"Debug: Parsed JSON successfully") else IO.unit
        debugMsg *> {
          json.as[JsonRpcMessage] match {
            case Right(message) =>
              val methodDebug = if (debug) IO.println(s"Debug: Handling method: ${message.method.getOrElse("response")}") else IO.unit
              methodDebug *> handleMessage(message, debug)
            case Left(error) =>
              IO.println(s"Failed to parse JSON-RPC message: $error")
          }
        }
      case Left(error) =>
        IO.println(s"Failed to parse JSON: $error")
    }
  }

  private def handleMessage(message: JsonRpcMessage, debug: Boolean = false): IO[Unit] = {
    message.method match {
      case Some("initialize") =>
        val debugMsg = if (debug) IO.println(s"Debug: Processing initialize request") else IO.unit
        debugMsg *> handleInitialize(message, debug)
      case Some("tools/list") =>
        val debugMsg = if (debug) IO.println(s"Debug: Processing tools/list request") else IO.unit
        debugMsg *> handleListTools(message, debug)
      case Some("tools/call") =>
        val debugMsg = if (debug) IO.println(s"Debug: Processing tools/call request") else IO.unit
        debugMsg *> handleCallTool(message, debug)
      case Some(method) =>
        val debugMsg = if (debug) IO.println(s"Debug: Unknown method: $method") else IO.unit
        debugMsg *> sendError(message.id, -32601, s"Method not found: $method", debug)
      case None =>
        val debugMsg = if (debug) IO.println(s"Debug: Received response message, ignoring") else IO.unit
        debugMsg
    }
  }

  private def handleInitialize(message: JsonRpcMessage, debug: Boolean = false): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: Creating initialize response") else IO.unit
    val response = InitializeResult(
      protocolVersion = "2025-06-18",
      capabilities = serverCapabilities,
      serverInfo = serverInfo
    )

    debugMsg *> sendResponse(message.id, response.asJson, debug)
  }

  private def handleListTools(message: JsonRpcMessage, debug: Boolean = false): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: Getting available tools") else IO.unit
    val tools = analysisTools.getAvailableTools()
    val toolsDebug = if (debug) IO.println(s"Debug: Found ${tools.length} tools") else IO.unit
    val response = ListToolsResult(tools)
    debugMsg *> toolsDebug *> sendResponse(message.id, response.asJson, debug)
  }

  private def handleCallTool(message: JsonRpcMessage, debug: Boolean = false): IO[Unit] = {
    message.params match {
      case Some(params) =>
        params.as[CallToolParams] match {
          case Right(request) =>
            val debugMsg = if (debug) IO.println(s"Debug: Calling tool '${request.name}' with args: ${request.arguments}") else IO.unit
            debugMsg *> analysisTools.callTool(request.name, request.arguments).flatMap { response =>
              val responseDebug = if (debug) IO.println(s"Debug: Tool call completed") else IO.unit
              responseDebug *> sendResponse(message.id, response.asJson, debug)
            }
          case Left(error) =>
            val debugMsg = if (debug) IO.println(s"Debug: Invalid tool call params: $error") else IO.unit
            debugMsg *> sendError(message.id, -32602, s"Invalid params: $error", debug)
        }
      case None =>
        val debugMsg = if (debug) IO.println(s"Debug: Missing params for tool call") else IO.unit
        debugMsg *> sendError(message.id, -32602, "Missing params", debug)
    }
  }

  private def sendResponse(id: Option[Either[String, Int]], result: Json, debug: Boolean = false): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: Sending response") else IO.unit
    val response = JsonRpcMessage(
      id = id,
      result = Some(result)
    )
    debugMsg *> IO.println(response.asJson.noSpaces)
  }

  private def sendError(id: Option[Either[String, Int]], code: Int, message: String, debug: Boolean = false): IO[Unit] = {
    val debugMsg = if (debug) IO.println(s"Debug: Sending error - code: $code, message: $message") else IO.unit
    val error = JsonRpcError(code, message)
    val response = JsonRpcMessage(
      id = id,
      error = Some(error)
    )
    debugMsg *> IO.println(response.asJson.noSpaces)
  }
}