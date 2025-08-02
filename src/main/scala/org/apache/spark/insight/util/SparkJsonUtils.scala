
package org.apache.spark.insight.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import java.text.SimpleDateFormat
import java.util.{Calendar, SimpleTimeZone}

/**
 * Utility class for serializing/deserializing Scala JSON objects
 */
object SparkJsonUtils {

  /**
   * Date format used in Spark History Server
   */
  val DATE_FORMAT: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }

  /**
   * ScalaObjectMapper for serializing/deserializing JSON objects
   */
  val SCALA_OBJECT_MAPPER: ObjectMapper with ScalaObjectMapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setDateFormat(DATE_FORMAT)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    SCALA_OBJECT_MAPPER.readValue[T](json)

}
