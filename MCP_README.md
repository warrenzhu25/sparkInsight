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

### 2. Configure Claude Desktop

Edit your Claude Desktop configuration file:

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Linux:** `~/.config/Claude/claude_desktop_config.json`

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

Replace `/absolute/path/to/` with the actual path to your JAR file.

### 3. Restart Claude Desktop

Restart Claude Desktop for the configuration to take effect.

## Available Tools

The MCP server provides these analysis tools:

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

## URL Formats

SparkInsight MCP server supports URLs from:

- **Spark History Server**: `http://localhost:18080/history/app-{app-id}`
- **Spark UI**: `http://localhost:4040/` (for running applications)
- **YARN ResourceManager**: `http://yarn-rm:8088/proxy/{app-id}/`

## Troubleshooting

### "Server not found" error
- Verify the JAR path in `claude_desktop_config.json` is absolute and correct
- Ensure you restarted Claude Desktop after configuration changes
- Check that Java is available in your PATH

### "Connection failed" error  
- Verify the Spark application URL is accessible
- Ensure the Spark History Server is running
- Check that the application ID exists

### Permission errors
- Ensure the JAR file has execute permissions
- Verify Java can access the JAR file location

## Integration with Other Tools

### Cursor IDE
To add SparkInsight to Cursor IDE, go to Settings > Tools & Integrations > New MCP Server and add:

```json
{
  "spark-insight": {
    "command": "java",
    "args": ["-jar", "/path/to/spark-insight-mcp-1.0-SNAPSHOT-jar-with-dependencies.jar"]
  }
}
```

### Continue.dev
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

## Protocol Details

SparkInsight implements the Model Context Protocol specification version 2025-06-18:

- **Transport**: Standard Input/Output (stdio)
- **Protocol**: JSON-RPC 2.0
- **Capabilities**: Tools only (no prompts or resources)

The server follows MCP best practices:
- Never writes to stdout except for JSON-RPC responses
- Provides detailed tool schemas with parameter validation
- Returns structured error messages for debugging
- Supports graceful shutdown

## Development

To extend the MCP server with additional tools:

1. Add new methods to `SparkAnalysisTools.scala`
2. Register tools in the `getAvailableTools()` method  
3. Implement tool logic in the `callTool()` method
4. Rebuild and test with Claude Desktop

## Contributing

Contributions to the MCP server are welcome! Please ensure:

- New tools follow the existing parameter schema patterns
- Error handling is comprehensive with helpful messages
- Tools provide meaningful analysis results
- Documentation is updated for new capabilities