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
}