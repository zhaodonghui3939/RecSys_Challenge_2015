package org.recsys.challenge.base

import org.apache.spark.ml.Model
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object BaseComputing extends Serializable{
  //获得click相关数据集合中session的记录集
  def getClickSessionData(clicks:RDD[String]) ={
    clicks.map{
      case record => {
        val ss = record.split(",")
        val key = ss(0)
        (key,record)
      }
    }.groupByKey().map{
      case (sessionId,records) =>{
        (sessionId,records.toArray.map(new ClickRecord(_)))
      }
    }
  }
  //获得buy数据集中session的记录集
  def getBuySessionData(buys:RDD[String]) = {
    buys.map{
      case record => {
        val ss = record.split(",")
        val key = ss(0)
        (key,record)
      }
    }.groupByKey().map{
      case (sessionId,records) =>{
        (sessionId,records.toArray.map(new BuyRecord(_)))
      }
    }
  }
  //获得click中item的数据集
  def getClickItemData(clicks:RDD[String]) ={
    clicks.map{
      case record => {
        val ss = record.split(",")
        val key = ss(2)
        (key,record)
      }
    }.groupByKey().map{
      case (sessionId,records) =>{
        (sessionId,records.toArray.map(new ClickRecord(_)))
      }
    }
  }
  //获得buy数据集中item的记录集
  def getBuyItemData(buys:RDD[String]) = {
    buys.map{
      case record => {
        val ss = record.split(",")
        val key = ss(2)
        (key,record)
      }
    }.groupByKey().map{
      case (sessionId,records) =>{
        (sessionId,records.toArray.map(new BuyRecord(_)))
      }
    }
  }

  def getSessionItemData(clicks:RDD[String]) = {
    clicks.map{
      case record => {
        val ss = record.split(",")
        val key = ss(0)+"_"+ss(2)
        (key,record)
      }
    }.groupByKey().map{
      case (sessionId,records) =>{
        (sessionId,records.toArray.map(new ClickRecord(_)))
      }
    }
  }

  def getSessionItemBuyPair(buys:RDD[String]) = {
    buys.map{
      case record => {
        val ss = record.split(",")
        val key = ss(0) + "_" + ss(2)
        (key)
      }
    }.distinct().collect().toSet
  }

  //构建labelpoint
  def toLabelPoint(features:RDD[(String,Array[Double])], pair:Set[String]):RDD[(String,LabeledPoint)] = {
    features.map{
      case(sessionItem,features) => {
        var s = new LabeledPoint(0.0,Vectors.dense(features))
        if(pair.contains(sessionItem)) s = new LabeledPoint(1.0,Vectors.dense(features))
        (sessionItem,s)
      }
    }
  }

  //对session特征 item特征进行join
  def joinFeatures(sessionItemFeatures:RDD[(String,Array[Double])],
                    sessionFeatures:RDD[(String,Array[Double])],
                    itemFeatures:RDD[(String,Array[Double])]): RDD[(String,Array[Double])] = {
    val item_features = itemFeatures.collect().toMap
    sessionItemFeatures.map{
      case (sessionItem,features) => {
        val session = sessionItem.split("_")(0)
        (session,(sessionItem,features))
      }
    }.join(sessionFeatures).map{   //和session特征进行join
      case(session,((sessionItem,features),sessionfeatures)) => {
        (sessionItem,features++sessionfeatures)
      }
    }.map(line => (line._1,line._2 ++ item_features(line._1.split("_")(1)))) //和item特征进行join
  }
}
