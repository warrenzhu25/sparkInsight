package org.apache.spark.insight.mcp.transport

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.comcast.ip4s._
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.mcp.tools.SparkAnalysisTools
import org.http4s._
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.http4s.server.middleware.CORS

import scala.concurrent.duration._

/**
 * HTTP transport for MCP server
 * Provides JSON-RPC over HTTP endpoints
 */
class HttpTransport(port: Int = 8080) {

  private val serverInfo = ServerInfo(
    name = "spark-insight-mcp",
    version = "1.0.0"
  )

  private val serverCapabilities = ServerCapabilities(
    tools = Some(ToolsCapability(listChanged = Some(false)))
  )

  private val analysisTools = new SparkAnalysisTools()

  private val corsConfig = CORS.policy
    .withAllowOriginAll
    .withAllowMethodsIn(Set(Method.GET, Method.POST, Method.OPTIONS))
    .withAllowCredentials(false)
    .withMaxAge(1.day)

  private val mcpRoutes = HttpRoutes.of[IO] {
    
    // Health check endpoint
    case GET -> Root / "health" =>
      Ok(Json.obj("status" -> Json.fromString("healthy")))

    // MCP endpoint for JSON-RPC requests
    case req @ POST -> Root / "mcp" =>
      req.as[JsonRpcMessage].flatMap(handleMessage).flatMap(Ok(_))

    // MCP endpoint for batch requests
    case req @ POST -> Root / "mcp" / "batch" =>
      req.as[List[JsonRpcMessage]].flatMap { messages =>
        messages.traverse(handleMessage).flatMap(results => Ok(results))
      }

    // CORS preflight
    case OPTIONS -> _ =>
      Ok()
  }

  private val httpApp = corsConfig(mcpRoutes).orNotFound

  def start(): Resource[IO, Server] = {
    EmberServerBuilder
      .default[IO]
      .withHost(ipv4"0.0.0.0")
      .withPort(Port.fromInt(port).getOrElse(port"8080"))
      .withHttpApp(httpApp)
      .build
  }

  private def handleMessage(message: JsonRpcMessage): IO[JsonRpcResponse] = {
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
        // Response message, convert to response format
        IO.pure(JsonRpcResponse(
          id = message.id,
          result = message.result,
          error = message.error
        ))
    }
  }

  private def handleInitialize(message: JsonRpcMessage): IO[JsonRpcResponse] = {
    val response = InitializeResult(
      protocolVersion = "2025-06-18",
      capabilities = serverCapabilities,
      serverInfo = serverInfo
    )

    IO.pure(JsonRpcResponse(
      id = message.id,
      result = Some(response.asJson)
    ))
  }

  private def handleListTools(message: JsonRpcMessage): IO[JsonRpcResponse] = {
    val tools = analysisTools.getAvailableTools()
    val response = ListToolsResult(tools)
    
    IO.pure(JsonRpcResponse(
      id = message.id,
      result = Some(response.asJson)
    ))
  }

  private def handleCallTool(message: JsonRpcMessage): IO[JsonRpcResponse] = {
    message.params match {
      case Some(params) =>
        params.as[CallToolParams] match {
          case Right(request) =>
            analysisTools.callTool(request.name, request.arguments).map { response =>
              JsonRpcResponse(
                id = message.id,
                result = Some(response.asJson)
              )
            }
          case Left(error) =>
            sendError(message.id, -32602, s"Invalid params: $error")
        }
      case None =>
        sendError(message.id, -32602, "Missing params")
    }
  }

  private def sendError(id: Option[Either[String, Int]], code: Int, message: String): IO[JsonRpcResponse] = {
    val error = JsonRpcError(code, message)
    IO.pure(JsonRpcResponse(
      id = id,
      error = Some(error)
    ))
  }
}