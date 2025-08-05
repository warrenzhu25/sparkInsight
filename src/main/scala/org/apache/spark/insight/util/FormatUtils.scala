package org.apache.spark.insight.util

import java.util.concurrent.TimeUnit

object FormatUtils {

  def formatValue(value: Long, isTime: Boolean, isNanoTime: Boolean, isSize: Boolean, isRecords: Boolean): String = {
    if (isTime) {
      s"${TimeUnit.MILLISECONDS.toMinutes(value)}"
    } else if (isNanoTime) {
      s"${TimeUnit.NANOSECONDS.toMinutes(value)}"
    } else if (isSize) {
      s"${value / (1024 * 1024 * 1024)}"
    } else if (isRecords) {
      s"${value / 1000}"
    } else {
      value.toString
    }
  }

  def formatBytes(bytes: Double): String = {
    if (bytes < 1024) {
      s"${bytes.toLong} B"
    } else if (bytes < 1024 * 1024) {
      f"${bytes / 1024}%.2f KB"
    } else if (bytes < 1024 * 1024 * 1024) {
      f"${bytes / (1024 * 1024)}%.2f MB"
    } else {
      f"${bytes / (1024 * 1024 * 1024)}%.2f GB"
    }
  }
}
