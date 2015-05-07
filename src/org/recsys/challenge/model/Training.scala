package org.recsys.challenge.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
object Training {

  def gbdt(features:RDD[LabeledPoint]) = {
    new GBDT(features).run
  }

  def rf(features:RDD[LabeledPoint]) = {
    new RF(features).run
  }

  def lr(features:RDD[LabeledPoint]) = {
    new LR(features).runLBFGS
  }

  def svmlr(features:RDD[LabeledPoint]) = {
    new SVM(features).run
  }
}
