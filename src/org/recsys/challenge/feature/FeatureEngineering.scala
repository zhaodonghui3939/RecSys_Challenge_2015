package org.recsys.challenge.feature

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.recsys.challenge.base.BaseComputing
import org.apache.spark.mllib.linalg.Vector

class FeatureEngineering(clicks:RDD[String],buys:RDD[String],test:RDD[String]) {
  private val itemFeature = ItemFeatures.getItemFeatures(BaseComputing.getClickItemData(clicks),BaseComputing.getBuyItemData(buys)).cache()
  //获得训练集的特征
  def getTraningFeatures:RDD[(String,LabeledPoint)] = {
    val sessionFeatures = SessionFeatures.getSessionFeatures(BaseComputing.getClickSessionData(clicks))
    val sessionItemFeatures = SessionItemFeatures.getSessionItemClickFeatures(BaseComputing.getSessionItemData(clicks))
    val features = BaseComputing.joinFeatures(sessionItemFeatures,sessionFeatures,itemFeature)
    val pair = BaseComputing.getSessionItemBuyPair(buys)
    BaseComputing.toLabelPoint(features,pair)
  }
  //获得预测的特征
  def getPredictFeatures:RDD[(String,LabeledPoint)] = {
    val sessionFeatures = SessionFeatures.getSessionFeatures(BaseComputing.getClickSessionData(test))
    val sessionItemFeatures = SessionItemFeatures.getSessionItemClickFeatures(BaseComputing.getSessionItemData(test))
    val features = BaseComputing.joinFeatures(sessionItemFeatures,sessionFeatures,itemFeature)
    BaseComputing.toLabelPoint(features,Set[String]())
  }
}
