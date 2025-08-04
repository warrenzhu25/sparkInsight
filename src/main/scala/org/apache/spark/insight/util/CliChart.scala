package org.apache.spark.insight.util

object CliChart {

  def barChart(data: Seq[(String, Int)], width: Int = 50): String = {
    if (data.isEmpty) {
      return ""
    }

    val maxValue = data.map(_._2).max
    val maxLabelWidth = data.map(_._1.length).max

    val sb = new StringBuilder
    data.foreach { case (label, value) =>
      val barWidth = if (maxValue == 0) 0 else (value.toDouble / maxValue * width).toInt
      val bar = "█" * barWidth
      val paddedLabel = label.padTo(maxLabelWidth, ' ').mkString
      sb.append(s"$paddedLabel | $bar $value\n")
    }
    sb.toString()
  }
}