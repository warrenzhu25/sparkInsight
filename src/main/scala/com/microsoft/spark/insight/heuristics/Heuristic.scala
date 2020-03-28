package com.microsoft.spark.insight.heuristics

import com.microsoft.spark.insight.fetcher.SparkApplicationData

trait Heuristic {
  val evaluators: Seq[SparkEvaluator] = Seq.empty
  val name: String = this.getClass.getSimpleName

  def apply(data: SparkApplicationData): HeuristicResult = {
    HeuristicResult(name, evaluators.flatMap(e => e.evaluate(data)))
  }
}
