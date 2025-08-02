package com.microsoft.spark.insight.server

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.spark.insight.analyzer.HtmlReportAnalyzer
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Type`
import org.http4s.{Header, HttpRoutes, MediaType}
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.implicits._
import org.apache.spark.insight.fetcher.SparkFetcher
import org.typelevel.ci.CIStringSyntax
import cats.implicits._

object WebServer {

  def main(args: Array[String]): Unit = {
    val server = BlazeServerBuilder[IO]
      .bindHttp(8080, "localhost")
      .withHttpApp(routes.orNotFound)
      .resource
      .use(_ => IO.never)
      .as(cats.effect.ExitCode.Success)
    server.unsafeRunSync()
  }

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(
        """
          |<!DOCTYPE html>
          |<html>
          |<head>
          |    <title>Spark Insight</title>
          |</head>
          |<body>
          |<h1>Spark Insight</h1>
          |<form action="/analyze" method="post">
          |    <label for="url1">Application URL 1:</label>
          |    <input type="text" id="url1" name="url1" required><br>
          |    <label for="url2">Application URL 2 (optional):</label>
          |    <input type="text" id="url2" name="url2"><br>
          |    <input type="submit" value="Analyze">
          |</form>
          |</body>
          |</html>
          |""".stripMargin, Header.Raw("Content-Type".ci, "text/html")
      )
    case req @ POST -> Root / "analyze" =>
      req.decode[org.http4s.UrlForm] { data =>
        val url1Opt = data.values.get("url1").flatMap(_.headOption)
        val url2Opt = data.values.get("url2").flatMap(_.headOption).filter(_.nonEmpty)

        (url1Opt, url2Opt) match {
          case (Some(url1), Some(url2)) =>
            val appData1 = SparkFetcher.fetchData(url1)
            val appData2 = SparkFetcher.fetchData(url2)
            val result = HtmlReportAnalyzer.analysis(appData1)
            Ok(result.rows.head.head, Header.Raw("Content-Type".ci, "text/html"))
          case (Some(url1), None) =>
            val appData = SparkFetcher.fetchData(url1)
            val result = HtmlReportAnalyzer.analysis(appData)
            Ok(result.rows.head.head, Header.Raw("Content-Type".ci, "text/html"))
          case _ =>
            BadRequest("Please provide at least one URL.")
        }
      }
  }
}
