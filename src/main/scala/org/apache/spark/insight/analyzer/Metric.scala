
package org.apache.spark.insight.analyzer

import org.apache.spark.status.api.v1.StageData

case class Metric(
    name: String,
    value: StageData => Long,
    description: String,
    isTime: Boolean = false,
    isNanoTime: Boolean = false,
    isSize: Boolean = false,
    isRecords: Boolean = false)
