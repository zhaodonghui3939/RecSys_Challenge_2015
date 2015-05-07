package org.recsys.challenge.base

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

  def joinFeatures(sessionItemFeatures:RDD[(String,Array[Double])],
                    sessionFeatures:RDD[(String,Array[Double])],
                    itemFeatures:RDD[(String,Array[Double])]): RDD[(String,Array[Double])] = {

    val item_features = itemFeatures.collect().toMap
    sessionItemFeatures.map(line => (line._1,line._2 ++ item_features(line._1.split("_")(1)))).map{
      case (sessionItem,features) => {
        val session = sessionItem.split("_")(0)
        (session,(sessionItem,features))
      }
    }.join(sessionFeatures).map{
      case(session,((sessionItem,features),sessionfeatures)) => {
        (sessionItem,features++sessionfeatures)
      }
    }
  }
}
