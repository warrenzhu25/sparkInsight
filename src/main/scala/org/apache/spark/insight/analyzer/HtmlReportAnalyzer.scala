package org.apache.spark.insight.analyzer

import org.apache.spark.insight.fetcher.SparkApplicationData
import scalatags.Text.all._

/**
 * An analyzer that generates an HTML report for a Spark application.
 */
object HtmlReportAnalyzer extends Analyzer {

  override def analysis(data: SparkApplicationData): AnalysisResult = {
    val htmlContent = html(
      head(
        link(rel := "stylesheet", href := "https://cdn.datatables.net/1.11.5/css/jquery.dataTables.css")
      ),
      body(
        h1(s"Spark Insight Report for ${data.appId}"),
        h2("Jobs"),
        table(id := "jobsTable", cls := "display")(
          thead(
            tr(
              th("Job ID"),
              th("Status"),
              th("Submission Time"),
              th("Completion Time"),
              th("Number of Stages"),
              th("Number of Tasks")
            )
          ),
          tbody(
            for (job <- data.jobData) yield {
              tr(
                td(job.jobId.toString),
                td(job.status.toString),
                td(job.submissionTime.toString),
                td(job.completionTime.toString),
                td(job.stageIds.size.toString),
                td(job.numTasks.toString)
              )
            }
          )
        ),
        h2("Stages"),
        table(id := "stagesTable", cls := "display")(
          thead(
            tr(
              th("Stage ID"),
              th("Name"),
              th("Status"),
              th("Number of Tasks"),
              th("Executor Run Time"),
              th("Input Bytes"),
              th("Output Bytes"),
              th("Shuffle Read Bytes"),
              th("Shuffle Write Bytes")
            )
          ),
          tbody(
            for (stage <- data.stageData) yield {
              tr(
                td(stage.stageId.toString),
                td(stage.name),
                td(stage.status.toString),
                td(stage.numTasks.toString),
                td(stage.executorRunTime.toString),
                td(stage.inputBytes.toString),
                td(stage.outputBytes.toString),
                td(stage.shuffleReadBytes.toString),
                td(stage.shuffleWriteBytes.toString)
              )
            }
          )
        ),
        h2("Executors"),
        table(id := "executorsTable", cls := "display")(
          thead(
            tr(
              th("ID"),
              th("Host:Port"),
              th("Is Active"),
              th("RDD Blocks"),
              th("Memory Used"),
              th("Disk Used"),
              th("Total Cores"),
              th("Max Tasks"),
              th("Active Tasks"),
              th("Failed Tasks"),
              th("Completed Tasks"),
              th("Total Tasks"),
              th("Total Duration"),
              th("Total Input Bytes"),
              th("Total Shuffle Read"),
              th("Total Shuffle Write")
            )
          ),
          tbody(
            for (executor <- data.executorSummaries) yield {
              tr(
                td(executor.id),
                td(executor.hostPort),
                td(executor.isActive.toString),
                td(executor.rddBlocks.toString),
                td(executor.memoryUsed.toString),
                td(executor.diskUsed.toString),
                td(executor.totalCores.toString),
                td(executor.maxTasks.toString),
                td(executor.activeTasks.toString),
                td(executor.failedTasks.toString),
                td(executor.completedTasks.toString),
                td(executor.totalTasks.toString),
                td(executor.totalDuration.toString),
                td(executor.totalInputBytes.toString),
                td(executor.totalShuffleRead.toString),
                td(executor.totalShuffleWrite.toString)
              )
            }
          )
        ),
        script(src := "https://code.jquery.com/jquery-3.6.0.min.js"),
        script(src := "https://cdn.datatables.net/1.11.5/js/jquery.dataTables.js"),
        script(raw(
          """
            |$(document).ready( function () {
            |    $('#jobsTable').DataTable();
            |    $('#stagesTable').DataTable();
            |    $('#executorsTable').DataTable();
            |} );
            |""".stripMargin
        ))
      )
    ).render

    AnalysisResult("HTML Report", Seq(), Seq(Seq(htmlContent)))
  }
}