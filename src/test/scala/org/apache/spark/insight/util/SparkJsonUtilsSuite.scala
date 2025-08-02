package org.apache.spark.insight.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

class SparkJsonUtilsSuite extends AnyFunSuite {

  test("SparkJsonUtils.SCALA_OBJECT_MAPPER should be a configured ObjectMapper") {
    val mapper = SparkJsonUtils.SCALA_OBJECT_MAPPER
    assert(mapper.isInstanceOf[ObjectMapper])
  }
}