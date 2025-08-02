
package com.microsoft.spark.insight.server

import org.apache.spark.insight.analyzer.AnalysisResult
import org.apache.spark.insight.fetcher.SparkApplicationData

object HtmlTemplates {

  def landingPage: String = {
    """
      |<!DOCTYPE html>
      |<html>
      |<head>
      |    <title>Spark Insight</title>
      |    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
      |</head>
      |<body>
      |  <main class="container">
      |    <h1>Spark Insight</h1>
      |    <form action="/analyze" method="post">
      |      <label for="url1">Application URL 1:</label>
      |      <input type="text" id="url1" name="url1" required><br>
      |      <label for="url2">Application URL 2 (optional):</label>
      |      <input type="text" id="url2" name="url2"><br>
      |      <button type="submit">Analyze</button>
      |    </form>
      |  </main>
      |</body>
      |</html>
      |""".stripMargin
  }

  def reportPage(data: SparkApplicationData): String = {
    s"""
      |<!DOCTYPE html>
      |<html>
      |<head>
      |    <title>Spark Insight Report</title>
      |    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
      |    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.css">
      |</head>
      |<body>
      |  <main class="container">
      |    <h1>Spark Insight Report for ${data.appId}</h1>
      |    <h2>Jobs</h2>
      |    <table id="jobsTable" class="display">
      |        <thead>
      |            <tr>
      |                <th>Job ID</th>
      |                <th>Status</th>
      |                <th>Submission Time</th>
      |                <th>Completion Time</th>
      |                <th>Number of Stages</th>
      |                <th>Number of Tasks</th>
      |            </tr>
      |        </thead>
      |        <tbody>
      |        ${
                data.jobData.map { job =>
                  s"""
                    |<tr>
                    |    <td>${job.jobId}</td>
                    |    <td>${job.status}</td>
                    |    <td>${job.submissionTime}</td>
                    |    <td>${job.completionTime}</td>
                    |    <td>${job.stageIds.size}</td>
                    |    <td>${job.numTasks}</td>
                    |</tr>
                    |""".stripMargin
                }.mkString
              }
      |        </tbody>
      |    </table>
      |    <h2>Stages</h2>
      |    <table id="stagesTable" class="display">
      |        <thead>
      |            <tr>
      |                <th>Stage ID</th>
      |                <th>Name</th>
      |                <th>Status</th>
      |                <th>Number of Tasks</th>
      |                <th>Executor Run Time</th>
      |                <th>Input Bytes</th>
      |                <th>Output Bytes</th>
      |                <th>Shuffle Read Bytes</th>
      |                <th>Shuffle Write Bytes</th>
      |            </tr>
      |        </thead>
      |        <tbody>
      |        ${
                data.stageData.map { stage =>
                  s"""
                    |<tr>
                    |    <td>${stage.stageId}</td>
                    |    <td>${stage.name}</td>
                    |    <td>${stage.status}</td>
                    |    <td>${stage.numTasks}</td>
                    |    <td>${stage.executorRunTime}</td>
                    |    <td>${stage.inputBytes}</td>
                    |    <td>${stage.outputBytes}</td>
                    |    <td>${stage.shuffleReadBytes}</td>
                    |    <td>${stage.shuffleWriteBytes}</td>
                    |</tr>
                    |""".stripMargin
                }.mkString
              }
      |        </tbody>
      |    </table>
      |    <h2>Executors</h2>
      |    <table id="executorsTable" class="display">
      |        <thead>
      |            <tr>
      |                <th>ID</th>
      |                <th>Host:Port</th>
      |                <th>Is Active</th>
      |                <th>RDD Blocks</th>
      |                <th>Memory Used</th>
      |                <th>Disk Used</th>
      |                <th>Total Coores</th>
      |                <th>Max Tasks</th>
      |                <th>Active Tasks</th>
      |                <th>Failed Tasks</th>
      |                <th>Completed Tasks</th>
      |                <th>Total Tasks</th>
      |                <th>Total Duration</th>
      |                <th>Total Input Bytes</th>
      |                <th>Total Shuffle Read</th>
      |                <th>Total Shuffle Write</th>
      |            </tr>
      |        </thead>
      |        <tbody>
      |        ${
                data.executorSummaries.map { executor =>
                  s"""
                    |<tr>
                    |    <td>${executor.id}</td>
                    |    <td>${executor.hostPort}</td>
                    |    <td>${executor.isActive}</td>
                    |    <td>${executor.rddBlocks}</td>
                    |    <td>${executor.memoryUsed}</td>
                    |    <td>${executor.diskUsed}</td>
                    |    <td>${executor.totalCores}</td>
                    |    <td>${executor.maxTasks}</td>
                    |    <td>${executor.activeTasks}</td>
                    |    <td>${executor.failedTasks}</td>
                    |    <td>${executor.completedTasks}</td>
                    |    <td>${executor.totalTasks}</td>
                    |    <td>${executor.totalDuration}</td>
                    |    <td>${executor.totalInputBytes}</td>
                    |    <td>${executor.totalShuffleRead}</td>
                    |    <td>${executor.totalShuffleWrite}</td>
                    |</tr>
                    |""".stripMargin
                }.mkString
              }
      |        </tbody>
      |    </table>
      |  </main>
      |  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
      |  <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.js"></script>
      |  <script>
      |    $$(document).ready( function () {
      |        $$('#jobsTable').DataTable();
      |        $$('#stagesTable').DataTable();
      |        $$('#executorsTable').DataTable();
      |    } );
      |  </script>
      |</body>
      |</html>
      |""".stripMargin
  }

  def diffReportPage(
      data1: SparkApplicationData,
      data2: SparkApplicationData,
      appDiff: AnalysisResult,
      stageDiff: AnalysisResult): String = {
    s"""
      |<!DOCTYPE html>
      |<html>
      |<head>
      |    <title>Spark Insight Diff Report</title>
      |    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
      |    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.css">
      |</head>
      |<body>
      |  <main class="container">
      |    <h1>Spark Insight Diff Report for ${data1.appId} and ${data2.appId}</h1>
      |    <h2>Application Diff</h2>
      |    <table id="appDiffTable" class="display">
      |        <thead>
      |            <tr>
      |                <th>${appDiff.header.mkString("</th><th>")}</th>
      |            </tr>
      |        </thead>
      |        <tbody>
      |        ${
                appDiff.rows.map { row =>
                  s"""
                    |<tr>
                    |    <td>${row.mkString("</td><td>")}</td>
                    |</tr>
                    |""".stripMargin
                }.mkString
              }
      |        </tbody>
      |    </table>
      |    <h2>Stage Level Diff</h2>
      |    <table id="stageDiffTable" class="display">
      |        <thead>
      |            <tr>
      |                <th>${stageDiff.header.mkString("</th><th>")}</th>
      |            </tr>
      |        </thead>
      |        <tbody>
      |        ${
                stageDiff.rows.map { row =>
                  s"""
                    |<tr>
                    |    <td>${row.mkString("</td><td>")}</td>
                    |</tr>
                    |""".stripMargin
                }.mkString
              }
      |        </tbody>
      |    </table>
      |  </main>
      |  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
      |  <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.js"></script>
      |  <script>
      |    $$(document).ready( function () {
      |        $$('#appDiffTable').DataTable();
      |        $$('#stageDiffTable').DataTable();
      |    } );
      |  </script>
      |</body>
      |</html>
      |""".stripMargin
  }
}
