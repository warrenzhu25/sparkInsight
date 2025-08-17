package org.apache.spark.insight.mcp

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._

/**
 * Model Context Protocol (MCP) JSON-RPC 2.0 message types and structures
 * Following MCP Specification 2025-06-18
 */
object McpProtocol {

  // Base JSON-RPC 2.0 message
  case class JsonRpcMessage(
    jsonrpc: String = "2.0",
    id: Option[Either[String, Int]] = None,
    method: Option[String] = None,
    params: Option[Json] = None,
    result: Option[Json] = None,
    error: Option[JsonRpcError] = None
  )

  // Response-only message without method/params fields
  case class JsonRpcResponse(
    jsonrpc: String = "2.0",
    id: Option[Either[String, Int]] = None,
    result: Option[Json] = None,
    error: Option[JsonRpcError] = None
  )

  case class JsonRpcError(
    code: Int,
    message: String,
    data: Option[Json] = None
  )

  // MCP Initialize messages
  case class InitializeParams(
    protocolVersion: String,
    capabilities: ClientCapabilities,
    clientInfo: ClientInfo
  )

  case class ClientCapabilities(
    experimental: Option[Json] = None,
    sampling: Option[SamplingCapability] = None
  )

  case class SamplingCapability()

  case class ClientInfo(
    name: String,
    version: String
  )

  case class InitializeResult(
    protocolVersion: String,
    capabilities: ServerCapabilities,
    serverInfo: ServerInfo
  )

  case class ServerCapabilities(
    experimental: Option[Json] = None,
    logging: Option[LoggingCapability] = None,
    prompts: Option[PromptsCapability] = None,
    resources: Option[ResourcesCapability] = None,
    tools: Option[ToolsCapability] = None
  )

  case class LoggingCapability()
  case class PromptsCapability(listChanged: Option[Boolean] = None)
  case class ResourcesCapability(subscribe: Option[Boolean] = None, listChanged: Option[Boolean] = None)
  case class ToolsCapability(listChanged: Option[Boolean] = None)

  case class ServerInfo(
    name: String,
    version: String
  )

  // Tool-related messages
  case class ListToolsParams()

  case class ListToolsResult(
    tools: List[Tool]
  )

  case class Tool(
    name: String,
    description: String,
    inputSchema: Json
  )

  case class CallToolParams(
    name: String,
    arguments: Option[Json] = None
  )

  case class CallToolResult(
    content: List[TextContent],
    isError: Option[Boolean] = None
  )

  case class TextContent(
    `type`: String = "text",
    text: String
  )

  // Implicit JSON encoders/decoders
  implicit val jsonRpcErrorEncoder: Encoder[JsonRpcError] = deriveEncoder
  implicit val jsonRpcErrorDecoder: Decoder[JsonRpcError] = deriveDecoder

  implicit val eitherStringIntEncoder: Encoder[Either[String, Int]] = Encoder.instance {
    case Left(s) => Json.fromString(s)
    case Right(i) => Json.fromInt(i)
  }

  implicit val eitherStringIntDecoder: Decoder[Either[String, Int]] = Decoder.instance { cursor =>
    cursor.as[String].map(Left(_)).left.flatMap(_ => cursor.as[Int].map(Right(_)))
  }

  implicit val jsonRpcMessageEncoder: Encoder[JsonRpcMessage] = deriveEncoder
  implicit val jsonRpcResponseEncoder: Encoder[JsonRpcResponse] = Encoder.instance { response =>
    val baseFields = List(
      "jsonrpc" -> Json.fromString(response.jsonrpc),
      "id" -> response.id.map {
        case Left(str) => Json.fromString(str)
        case Right(num) => Json.fromInt(num)
      }.getOrElse(Json.Null)
    )
    
    val resultField = response.result.toList.map("result" -> _)
    val errorField = response.error.toList.map("error" -> _.asJson)
    
    Json.obj((baseFields ++ resultField ++ errorField): _*)
  }
  implicit val jsonRpcMessageDecoder: Decoder[JsonRpcMessage] = deriveDecoder

  implicit val samplingCapabilityEncoder: Encoder[SamplingCapability] = deriveEncoder
  implicit val samplingCapabilityDecoder: Decoder[SamplingCapability] = deriveDecoder

  implicit val clientCapabilitiesEncoder: Encoder[ClientCapabilities] = deriveEncoder
  implicit val clientCapabilitiesDecoder: Decoder[ClientCapabilities] = deriveDecoder

  implicit val clientInfoEncoder: Encoder[ClientInfo] = deriveEncoder
  implicit val clientInfoDecoder: Decoder[ClientInfo] = deriveDecoder

  implicit val initializeParamsEncoder: Encoder[InitializeParams] = deriveEncoder
  implicit val initializeParamsDecoder: Decoder[InitializeParams] = deriveDecoder

  implicit val loggingCapabilityEncoder: Encoder[LoggingCapability] = deriveEncoder
  implicit val loggingCapabilityDecoder: Decoder[LoggingCapability] = deriveDecoder

  implicit val promptsCapabilityEncoder: Encoder[PromptsCapability] = deriveEncoder
  implicit val promptsCapabilityDecoder: Decoder[PromptsCapability] = deriveDecoder

  implicit val resourcesCapabilityEncoder: Encoder[ResourcesCapability] = deriveEncoder
  implicit val resourcesCapabilityDecoder: Decoder[ResourcesCapability] = deriveDecoder

  implicit val toolsCapabilityEncoder: Encoder[ToolsCapability] = deriveEncoder
  implicit val toolsCapabilityDecoder: Decoder[ToolsCapability] = deriveDecoder

  implicit val serverCapabilitiesEncoder: Encoder[ServerCapabilities] = deriveEncoder
  implicit val serverCapabilitiesDecoder: Decoder[ServerCapabilities] = deriveDecoder

  implicit val serverInfoEncoder: Encoder[ServerInfo] = deriveEncoder
  implicit val serverInfoDecoder: Decoder[ServerInfo] = deriveDecoder

  implicit val initializeResultEncoder: Encoder[InitializeResult] = deriveEncoder
  implicit val initializeResultDecoder: Decoder[InitializeResult] = deriveDecoder

  implicit val listToolsParamsEncoder: Encoder[ListToolsParams] = deriveEncoder
  implicit val listToolsParamsDecoder: Decoder[ListToolsParams] = deriveDecoder

  implicit val toolEncoder: Encoder[Tool] = deriveEncoder
  implicit val toolDecoder: Decoder[Tool] = deriveDecoder

  implicit val listToolsResultEncoder: Encoder[ListToolsResult] = deriveEncoder
  implicit val listToolsResultDecoder: Decoder[ListToolsResult] = deriveDecoder

  implicit val callToolParamsEncoder: Encoder[CallToolParams] = deriveEncoder
  implicit val callToolParamsDecoder: Decoder[CallToolParams] = deriveDecoder

  implicit val textContentEncoder: Encoder[TextContent] = deriveEncoder
  implicit val textContentDecoder: Decoder[TextContent] = deriveDecoder

  implicit val callToolResultEncoder: Encoder[CallToolResult] = deriveEncoder
  implicit val callToolResultDecoder: Decoder[CallToolResult] = deriveDecoder
}