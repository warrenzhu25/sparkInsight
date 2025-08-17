package org.apache.spark.insight.mcp.transport

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.{Stream}
import fs2.concurrent.Topic
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.insight.mcp.McpProtocol._
import org.apache.spark.insight.mcp.tools.SparkAnalysisTools
import org.http4s._
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.`Content-Type`
import org.typelevel.ci._
import org.http4s.server.Server
import org.http4s.server.middleware.CORS

import java.util.UUID
import scala.concurrent.duration._

/**
 * Server-Sent Events (SSE) transport for MCP server
 * Provides bidirectional communication using SSE for server-to-client and HTTP POST for client-to-server
 */
class SseTransport(port: Int = 8081) {

  private val serverInfo = ServerInfo(
    name = "spark-insight-mcp",
    version = "1.0.0"
  )

  private val serverCapabilities = ServerCapabilities(
    tools = Some(ToolsCapability(listChanged = Some(false)))
  )

  private val analysisTools = new SparkAnalysisTools()

  // Store active SSE connections
  case class SseConnection(id: String, topic: Topic[IO, Option[JsonRpcMessage]])

  private val corsConfig = CORS.policy
    .withAllowOriginAll
    .withAllowMethodsIn(Set(Method.GET, Method.POST, Method.OPTIONS))
    .withAllowCredentials(false)
    .withMaxAge(1.day)

  def start(): Resource[IO, Server] = {
    for {
      connections <- Resource.eval(Ref.of[IO, Map[String, SseConnection]](Map.empty))
      server <- createServer(connections)
    } yield server
  }

  private def createServer(connections: Ref[IO, Map[String, SseConnection]]): Resource[IO, Server] = {
    val sseRoutes = HttpRoutes.of[IO] {
      
      // Health check endpoint
      case GET -> Root / "health" =>
        Ok(Json.obj("status" -> Json.fromString("healthy")))

      // SSE endpoint for receiving server messages
      case GET -> Root / "mcp" / "events" =>
        establishSseConnection(connections)

      // HTTP endpoint for sending client messages
      case req @ POST -> Root / "mcp" / "message" / connectionId =>
        handleClientMessage(connections, connectionId, req)

      // Get connection info
      case GET -> Root / "mcp" / "connections" =>
        connections.get.flatMap { conns =>
          val connectionInfo = conns.keys.toList.map { id =>
            Json.obj("id" -> Json.fromString(id))
          }
          Ok(Json.obj("connections" -> Json.arr(connectionInfo: _*)))
        }

      // CORS preflight
      case OPTIONS -> _ =>
        Ok()
    }

    val httpApp = corsConfig(sseRoutes).orNotFound

    EmberServerBuilder
      .default[IO]
      .withHost(ipv4"0.0.0.0")
      .withPort(Port.fromInt(port).getOrElse(port"8081"))
      .withHttpApp(httpApp)
      .build
  }

  private def establishSseConnection(connections: Ref[IO, Map[String, SseConnection]]): IO[Response[IO]] = {
    val connectionId = UUID.randomUUID().toString
    
    for {
      topic <- Topic[IO, Option[JsonRpcMessage]]
      connection = SseConnection(connectionId, topic)
      _ <- connections.update(_ + (connectionId -> connection))
      
      // Send initial connection established event
      initialEvent = ServerSentEvent(
        data = Some(Json.obj(
          "type" -> Json.fromString("connection"),
          "connectionId" -> Json.fromString(connectionId),
          "message" -> Json.fromString("Connection established")
        ).noSpaces),
        eventType = Some("connection"),
        id = Some(ServerSentEvent.EventId(connectionId))
      )

      sseStream = Stream.emit(initialEvent) ++ 
        topic.subscribe(10)
          .unNone
          .map(messageToSse)
          .handleErrorWith { error =>
            val errorEvent = ServerSentEvent(
              data = Some(Json.obj(
                "type" -> Json.fromString("error"),
                "message" -> Json.fromString(error.getMessage)
              ).noSpaces),
              eventType = Some("error")
            )
            Stream.emit(errorEvent)
          }
          .onFinalize {
            connections.update(_ - connectionId)
          }
      
      response <- Ok(sseStream)
        .map(_.withContentType(`Content-Type`(MediaType.unsafeParse("text/event-stream"))))
        .map(_.putHeaders(
          Header.Raw(CIString("Cache-Control"), "no-cache"),
          Header.Raw(CIString("Connection"), "keep-alive")
        ))
    } yield response
  }

  private def handleClientMessage(
    connections: Ref[IO, Map[String, SseConnection]], 
    connectionId: String, 
    req: Request[IO]
  ): IO[Response[IO]] = {
    connections.get.flatMap { conns =>
      conns.get(connectionId) match {
        case Some(connection) =>
          req.as[JsonRpcMessage].flatMap { message =>
            handleMessage(message).flatMap { response =>
              connection.topic.publish1(Some(response)) >> Ok(response)
            }
          }
        case None =>
          BadRequest(Json.obj("error" -> Json.fromString(s"Connection $connectionId not found")))
      }
    }
  }

  private def messageToSse(message: JsonRpcMessage): ServerSentEvent = {
    ServerSentEvent(
      data = Some(message.asJson.noSpaces),
      eventType = message.method.map(_ => "request").orElse(Some("response")),
      id = message.id.map(_.fold(identity, _.toString)).map(ServerSentEvent.EventId(_))
    )
  }

  private def handleMessage(message: JsonRpcMessage): IO[JsonRpcMessage] = {
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
        // Response message, echo back
        IO.pure(message)
    }
  }

  private def handleInitialize(message: JsonRpcMessage): IO[JsonRpcMessage] = {
    val response = InitializeResult(
      protocolVersion = "2025-06-18",
      capabilities = serverCapabilities,
      serverInfo = serverInfo
    )

    IO.pure(JsonRpcMessage(
      id = message.id,
      result = Some(response.asJson)
    ))
  }

  private def handleListTools(message: JsonRpcMessage): IO[JsonRpcMessage] = {
    val tools = analysisTools.getAvailableTools()
    val response = ListToolsResult(tools)
    
    IO.pure(JsonRpcMessage(
      id = message.id,
      result = Some(response.asJson)
    ))
  }

  private def handleCallTool(message: JsonRpcMessage): IO[JsonRpcMessage] = {
    message.params match {
      case Some(params) =>
        params.as[CallToolParams] match {
          case Right(request) =>
            analysisTools.callTool(request.name, request.arguments).map { response =>
              JsonRpcMessage(
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

  private def sendError(id: Option[Either[String, Int]], code: Int, message: String): IO[JsonRpcMessage] = {
    val error = JsonRpcError(code, message)
    IO.pure(JsonRpcMessage(
      id = id,
      error = Some(error)
    ))
  }
}