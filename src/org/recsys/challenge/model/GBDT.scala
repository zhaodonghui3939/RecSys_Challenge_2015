package org.recsys.challenge.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD

class GBDT(data: RDD[LabeledPoint]) extends Serializable{
  def run = {
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Regression")
    //val categoricalFeaturesInfo = Map[Int, Int]((3,16),(4,13),(5,32),(6,8))
    boostingStrategy.setNumIterations(60)
    boostingStrategy.treeStrategy.setMaxDepth(6)
   // boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(categoricalFeaturesInfo)
    GradientBoostedTrees.train(data, boostingStrategy)
  }
}
