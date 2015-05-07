package org.recsys.challenge.model

import org.apache.spark.mllib.classification.{LogisticRegressionModel, SVMModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD

object Predict {

  //预测
  def predictRF(data:RDD[(String,LabeledPoint)],model:RandomForestModel,num:Int) = {
    data.map{
      case(sessionItem,featuresV) => {
        (model.predict(featuresV.features),(sessionItem,featuresV.label))
      }
    }.top(num).map(_._2)
  }

  def predictGBDT(data:RDD[(String,LabeledPoint)],model:GradientBoostedTreesModel,num:Int) = {
    data.map{
      case(sessionItem,featuresV) => {
        (model.predict(featuresV.features),(sessionItem,featuresV.label))
      }
    }.top(num).map(_._2)
  }

  def predictSVM(data:RDD[(String,LabeledPoint)],model:SVMModel,num:Int) = {
    data.map { case (userItem, LabeledPoint(label, features)) =>
      val prediction = model.predict(Vectors.dense(features.toArray.map(line => Math.log(line + 1)))) //做了log处理
      (prediction, (userItem, label))
    }.top(num).map(_._2)
  }

  def lrPredict(data: RDD[(String, LabeledPoint)], model: LogisticRegressionModel, num: Int): Array[(String, Double)] = {
    data.map { case (userItem, LabeledPoint(label, features)) =>
      val prediction = model.clearThreshold().predict(Vectors.dense(features.toArray.map(line => Math.log(line + 1))))
      (prediction, (userItem, label))
    }.top(num).map(_._2)
  }
}
