package org.apache.spark.insight.mcp

import cats.effect.{IO, IOApp}
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.mcp.tools.SparkAnalysisTools

import scala.io.StdIn

/**
 * MCP Server implementation for SparkInsight
 * Provides Model Context Protocol interface for AI assistants to analyze Spark applications
 * Following MCP Specification 2025-06-18
 */
object McpServer extends IOApp.Simple {

  private val serverInfo = ServerInfo(
    name = "spark-insight-mcp",
    version = "1.0.0"
  )

  private val serverCapabilities = ServerCapabilities(
    tools = Some(ToolsCapability(listChanged = Some(false)))
  )

  private val analysisTools = new SparkAnalysisTools()

  def run: IO[Unit] = {
    processMessages()
  }

  private def processMessages(): IO[Unit] = {
    for {
      line <- IO(StdIn.readLine())
      _ <- if (line != null) {
        for {
          _ <- processMessage(line)
          _ <- processMessages()
        } yield ()
      } else {
        IO.unit
      }
    } yield ()
  }

  private def processMessage(line: String): IO[Unit] = {
    parser.parse(line) match {
      case Right(json) =>
        json.as[JsonRpcMessage] match {
          case Right(message) =>
            handleMessage(message)
          case Left(error) =>
            IO.println(s"Failed to parse JSON-RPC message: $error")
        }
      case Left(error) =>
        IO.println(s"Failed to parse JSON: $error")
    }
  }

  private def handleMessage(message: JsonRpcMessage): IO[Unit] = {
    message.method match {
      case Some("initialize") =>
        handleInitialize(message)
      case Some("tools/list") =>
        handleListTools(message)
      case Some("tools/call") =>
        handleCallTool(message)
      case Some(method) =>
        sendError(message.id, -32601, s"Method not found: $method")
      case None =>
        // Response message, ignore for now
        IO.unit
    }
  }

  private def handleInitialize(message: JsonRpcMessage): IO[Unit] = {
    val response = InitializeResult(
      protocolVersion = "2025-06-18",
      capabilities = serverCapabilities,
      serverInfo = serverInfo
    )

    sendResponse(message.id, response.asJson)
  }

  private def handleListTools(message: JsonRpcMessage): IO[Unit] = {
    val tools = analysisTools.getAvailableTools()
    val response = ListToolsResult(tools)
    sendResponse(message.id, response.asJson)
  }

  private def handleCallTool(message: JsonRpcMessage): IO[Unit] = {
    message.params match {
      case Some(params) =>
        params.as[CallToolParams] match {
          case Right(request) =>
            analysisTools.callTool(request.name, request.arguments).flatMap { response =>
              sendResponse(message.id, response.asJson)
            }
          case Left(error) =>
            sendError(message.id, -32602, s"Invalid params: $error")
        }
      case None =>
        sendError(message.id, -32602, "Missing params")
    }
  }

  private def sendResponse(id: Option[Either[String, Int]], result: Json): IO[Unit] = {
    val response = JsonRpcMessage(
      id = id,
      result = Some(result)
    )
    IO.println(response.asJson.noSpaces)
  }

  private def sendError(id: Option[Either[String, Int]], code: Int, message: String): IO[Unit] = {
    val error = JsonRpcError(code, message)
    val response = JsonRpcMessage(
      id = id,
      error = Some(error)
    )
    IO.println(response.asJson.noSpaces)
  }
}