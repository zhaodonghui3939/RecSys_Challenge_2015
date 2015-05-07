package org.recsys.challenge.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD

class GBDT(data: RDD[LabeledPoint]) extends Serializable{
  def run = {
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(20)
    boostingStrategy.treeStrategy.setMaxDepth(6)
    GradientBoostedTrees.train(data, boostingStrategy)
  }
}
