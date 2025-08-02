
package com.microsoft.spark.insight.server

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Type`
import org.http4s.{Header, HttpRoutes, MediaType}
import org.http4s.ember.server.EmberServerBuilder
import com.comcast.ip4s._
import org.http4s.implicits._
import org.apache.spark.insight.analyzer.{AppDiffAnalyzer, StageLevelDiffAnalyzer}
import org.apache.spark.insight.fetcher.SparkFetcher
import org.typelevel.ci.CIStringSyntax
import cats.implicits._
import com.microsoft.spark.insight.server.HtmlTemplates

object WebServer {

  def main(args: Array[String]): Unit = {
    val server = EmberServerBuilder.default[IO]
      .withHost(host"localhost")
      .withPort(port"8080")
      .withHttpApp(routes.orNotFound)
      .build
      .use(_ => IO.never)
      .as(cats.effect.ExitCode.Success)
    server.unsafeRunSync()
  }

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(HtmlTemplates.landingPage, Header.Raw("Content-Type".ci, "text/html"))
    case req @ POST -> Root / "analyze" =>
      req.decode[org.http4s.UrlForm] { data =>
        val url1Opt = data.values.get("url1").flatMap(_.headOption)
        val url2Opt = data.values.get("url2").flatMap(_.headOption).filter(_.nonEmpty)

        (url1Opt, url2Opt) match {
          case (Some(url1), Some(url2)) =>
            val appData1 = SparkFetcher.fetchData(url1)
            val appData2 = SparkFetcher.fetchData(url2)
            val appDiff = AppDiffAnalyzer.analysis(appData1, appData2)
            val stageDiff = StageLevelDiffAnalyzer.analysis(appData1, appData2)
            Ok(HtmlTemplates.diffReportPage(appData1, appData2, appDiff, stageDiff),
                Header.Raw("Content-Type".ci, "text/html"))
          case (Some(url1), None) =>
            val appData = SparkFetcher.fetchData(url1)
            Ok(HtmlTemplates.reportPage(appData), Header.Raw("Content-Type".ci, "text/html"))
          case _ =>
            BadRequest("Please provide at least one URL.")
        }
      }
  }
}
