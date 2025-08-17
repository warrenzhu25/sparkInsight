# SparkInsight MCP Server

SparkInsight now includes a Model Context Protocol (MCP) server that allows AI assistants like Claude to directly analyze Spark applications. This enables natural language queries about Spark performance and automated insights.

## What is MCP?

The Model Context Protocol (MCP) is an open standard that enables AI assistants to securely connect to external tools and data sources. With SparkInsight's MCP server, you can ask questions like:

- "Analyze the performance of my Spark application"
- "Compare these two Spark apps and identify regressions" 
- "What's causing shuffle skew in my job?"
- "How can I optimize executor utilization?"

## Quick Start

### 1. Build the MCP Server

```bash
mvn clean package
```

This creates two JAR files:
- `target/spark-insight-cli-1.0-SNAPSHOT-jar-with-dependencies.jar` - Traditional CLI
- `target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar` - MCP Server

### 1.1. Run the MCP Server

The MCP server supports three transport modes:

**Standard I/O (stdio) - Default Mode:**
```bash
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar
```

**HTTP Mode:**
```bash
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar --transport http --http-port 8080
```

**Server-Sent Events (SSE) Mode:**
```bash
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar --transport sse --sse-port 8081
```

**Command Line Options:**
```bash
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar --help
```

### 2. Configure Claude Desktop

Edit your Claude Desktop configuration file:

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Linux:** `~/.config/Claude/claude_desktop_config.json`

#### 2.1. Standard I/O Mode (Default)

Add the SparkInsight MCP server:

```json
{
  "mcpServers": {
    "spark-insight": {
      "command": "java",
      "args": [
        "-jar", 
        "/absolute/path/to/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar"
      ]
    }
  }
}
```

#### 2.2. HTTP Mode Configuration

For HTTP mode, you can use tools like curl or web clients:

```bash
# Start the HTTP server
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar --transport http --http-port 8080

# Test with curl
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

#### 2.3. Server-Sent Events (SSE) Mode

SSE mode provides real-time bidirectional communication:

```bash
# Start the SSE server
java -jar target/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar --transport sse --sse-port 8081

# Connect to SSE endpoint (in browser or with curl)
curl -N http://localhost:8081/mcp/events

# Send messages (in another terminal)
curl -X POST http://localhost:8081/mcp/message/{connectionId} \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Replace `/absolute/path/to/` with the actual path to your JAR file.

### 3. Restart Claude Desktop

Restart Claude Desktop for the configuration to take effect.

## Available Tools

The MCP server provides these analysis tools:

## High-Level Analysis Tools

### `analyze_spark_app`
Analyze a single Spark application for performance insights and recommendations.

**Parameters:**
- `url` (required): Spark application tracking URL
- `analyzers` (optional): List of analyzers to run: `["app_summary", "auto_scaling", "shuffle_skew", "executor"]`

**Example:**
```
Analyze the Spark app at http://localhost:18080/history/app-20240228220418-0000
```

### `compare_spark_apps` 
Compare two Spark applications to identify performance differences.

**Parameters:**
- `url1` (required): First Spark application URL
- `url2` (required): Second Spark application URL

**Example:**
```
Compare these two Spark applications and identify any regressions:
- App 1: http://localhost:18080/history/app-20240228220418-0000  
- App 2: http://localhost:18080/history/app-20240228220419-0001
```

### `get_app_summary`
Get a quick summary of Spark application metrics and basic statistics.

**Parameters:**
- `url` (required): Spark application tracking URL

### `analyze_shuffle_skew`
Analyze shuffle operations for data skew and performance bottlenecks.

**Parameters:**  
- `url` (required): Spark application tracking URL

### `analyze_executor_usage`
Analyze executor resource utilization and identify optimization opportunities.

**Parameters:**
- `url` (required): Spark application tracking URL

## Low-Level Data Access Tools

### `list_applications`
List all applications available in the Spark History Server.

**Parameters:**
- `history_server_url` (optional): Spark History Server URL (default: http://localhost:18080)

**Example:**
```
List all applications available in the history server
```

### `get_executors`
Get detailed executor information for a Spark application.

**Parameters:**
- `url` (required): Spark application tracking URL
- `active_only` (optional): Return only active executors (default: false)

**Example:**
```
Get executor details for my Spark app including memory and CPU usage
```

### `get_application_info`
Get detailed application metadata and configuration.

**Parameters:**
- `url` (required): Spark application tracking URL

**Example:**
```
Show me the configuration and metadata for this Spark application
```

### `get_jobs_info`
Get detailed job information for a Spark application.

**Parameters:**
- `url` (required): Spark application tracking URL

**Example:**
```
Show me all jobs in this application with their status and task counts
```

### `get_stages_info`
Get detailed stage information and metrics for a Spark application.

**Parameters:**
- `url` (required): Spark application tracking URL

**Example:**
```
Get detailed stage-level metrics including input/output and shuffle data
```

## Usage Examples

Once configured with Claude Desktop, you can ask natural language questions:

### Basic Analysis
```
"Please analyze my Spark application at http://localhost:18080/history/app-20240228220418-0000 and provide performance recommendations."
```

### Comparison Analysis  
```
"I ran the same job twice with different configurations. Can you compare them and tell me which performed better?
- Before: http://localhost:18080/history/app-20240228220418-0000
- After: http://localhost:18080/history/app-20240228220419-0001"
```

### Specific Analysis
```
"Check for shuffle skew in this Spark application: http://localhost:18080/history/app-20240228220418-0000"
```

### Data Exploration
```
"List all applications in the history server and show me the most recent ones"
```

```
"Show me detailed executor information for my application, including which ones are still active"
```

```
"Get the complete configuration and job breakdown for this Spark application"
```

### Granular Investigation
```
"Show me stage-by-stage metrics for this application to identify bottlenecks"
```

```
"List all jobs in this application and tell me which ones failed"
```

## Transport Modes

### Standard I/O (stdio)
- **Use case**: MCP clients like Claude Desktop, Cursor IDE
- **Protocol**: JSON-RPC over stdin/stdout
- **Configuration**: Add to MCP client configuration
- **Port**: None (uses process pipes)

### HTTP Mode
- **Use case**: Web applications, REST API clients, testing
- **Protocol**: JSON-RPC over HTTP
- **Default Port**: 8080
- **Endpoints**:
  - `POST /mcp` - Single JSON-RPC requests
  - `POST /mcp/batch` - Batch JSON-RPC requests
  - `GET /health` - Health check
- **CORS**: Enabled for web browsers

### Server-Sent Events (SSE) Mode
- **Use case**: Real-time web applications, dashboards
- **Protocol**: SSE + HTTP POST for bidirectional communication
- **Default Port**: 8081
- **Endpoints**:
  - `GET /mcp/events` - Establish SSE connection
  - `POST /mcp/message/{connectionId}` - Send messages
  - `GET /mcp/connections` - List active connections
  - `GET /health` - Health check
- **Features**: Connection management, real-time streaming

## URL Formats

SparkInsight MCP server supports URLs from:

- **Spark History Server**: `http://localhost:18080/history/app-{app-id}`
- **Spark UI**: `http://localhost:4040/` (for running applications)
- **YARN ResourceManager**: `http://yarn-rm:8088/proxy/{app-id}/`

## Troubleshooting

### Standard I/O Mode

**"Server not found" error:**
- Verify the JAR path in `claude_desktop_config.json` is absolute and correct
- Ensure you restarted Claude Desktop after configuration changes
- Check that Java is available in your PATH

**Permission errors:**
- Ensure the JAR file has execute permissions
- Verify Java can access the JAR file location

### HTTP Mode

**"Connection refused" error:**
- Verify the server is running: check console output
- Ensure the port is not in use by another application
- Check firewall settings

**CORS errors in browser:**
- CORS is enabled by default, but ensure the request includes proper headers
- Use `Content-Type: application/json` for POST requests

### SSE Mode

**SSE connection fails:**
- Verify the server is running on the specified port
- Check browser developer tools for connection errors
- Ensure the connection ID is valid when sending messages

**Message delivery issues:**
- Verify the connection ID exists: `GET /mcp/connections`
- Check that the SSE connection is still active
- Monitor server logs for error messages

### General Issues

**"Connection failed" error:**
- Verify the Spark application URL is accessible
- Ensure the Spark History Server is running
- Check that the application ID exists

**Java version mismatch:**
- Ensure you're using Java 8 or higher
- Check: `java -version`

## Integration with Other Tools

### MCP Clients (stdio mode)

**Cursor IDE:**
To add SparkInsight to Cursor IDE, go to Settings > Tools & Integrations > New MCP Server and add:

```json
{
  "spark-insight": {
    "command": "java",
    "args": ["-jar", "/path/to/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar"]
  }
}
```

**Continue.dev:**
Add to your `continue_config.json`:

```json
{
  "mcpServers": {
    "spark-insight": {
      "command": "java", 
      "args": ["-jar", "/path/to/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar"]
    }
  }
}
```

### Web Applications (HTTP mode)

**JavaScript/TypeScript:**
```javascript
const response = await fetch('http://localhost:8080/mcp', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    jsonrpc: '2.0',
    id: 1,
    method: 'tools/list'
  })
});
const result = await response.json();
```

**Python:**
```python
import requests

response = requests.post('http://localhost:8080/mcp', json={
    'jsonrpc': '2.0',
    'id': 1,
    'method': 'tools/list'
})
result = response.json()
```

### Real-time Applications (SSE mode)

**JavaScript EventSource:**
```javascript
const eventSource = new EventSource('http://localhost:8081/mcp/events');
eventSource.onmessage = function(event) {
  const data = JSON.parse(event.data);
  console.log('Received:', data);
};

// Send message
fetch(`http://localhost:8081/mcp/message/${connectionId}`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    jsonrpc: '2.0',
    id: 1,
    method: 'analyze_spark_app',
    params: { url: 'http://localhost:18080/history/app-20240228220418-0000' }
  })
});
```

## Protocol Details

SparkInsight implements the Model Context Protocol specification version 2025-06-18:

- **Transport**: Standard Input/Output (stdio), HTTP, Server-Sent Events (SSE)
- **Protocol**: JSON-RPC 2.0
- **Capabilities**: Tools only (no prompts or resources)

The server follows MCP best practices:
- Never writes to stdout except for JSON-RPC responses (stdio mode)
- Provides detailed tool schemas with parameter validation
- Returns structured error messages for debugging
- Supports graceful shutdown
- CORS enabled for web client compatibility (HTTP/SSE modes)
- Connection management and real-time streaming (SSE mode)

## Development

To extend the MCP server with additional tools:

1. Add new methods to `SparkAnalysisTools.scala`
2. Register tools in the `getAvailableTools()` method  
3. Implement tool logic in the `callTool()` method
4. Rebuild: `mvn clean package`
5. Test with your preferred transport mode:
   - **stdio**: Test with Claude Desktop or other MCP clients
   - **HTTP**: Test with curl or web applications  
   - **SSE**: Test with web browsers or real-time applications

## Alternative Libraries

For new projects, consider these Scala MCP libraries:

- **fastmcp-scala**: Annotation-driven Scala 3 library with ZIO support
  ```scala
  //> using dep com.tjclp::fast-mcp-scala:0.1.1
  
  @Tool(name = Some("analyze"))
  def analyzeApp(@ToolParam("url") url: String): String = { ... }
  ```

- **indoorvivants/mcp**: Alternative Scala MCP implementation

SparkInsight's current implementation provides full MCP compatibility across all transport modes while maintaining Scala 2.13 compatibility.

## Contributing

Contributions to the MCP server are welcome! Please ensure:

- New tools follow the existing parameter schema patterns
- Error handling is comprehensive with helpful messages
- Tools provide meaningful analysis results
- Documentation is updated for new capabilities
- Test across all transport modes (stdio, HTTP, SSE)
- Follow existing code patterns and conventions