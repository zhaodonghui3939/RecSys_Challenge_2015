package org.recsys.challenge

import org.apache.spark.SparkContext
import org.recsys.challenge.base.BaseComputing
import org.recsys.challenge.feature.{ItemFeatures, SessionFeatures}

object RecSysMain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val clicks = sc.textFile("/data/recsys2015/yoochoose-clicks.dat").cache()
    val buys = sc.textFile("/data/recsys2015/yoochoose-buys.dat").cache()
    val test = sc.textFile("/data/recsys2015/yoochoose-test.dat").cache()

    val ss = BaseComputing.getClickSessionData(clicks).cache()

    val sf = SessionFeatures.getSessionFeatures(ss)

    val itemClickSet = BaseComputing.getClickItemData(clicks)
    val itemBuySet = BaseComputing.getBuyItemData(buys)

    val itemFeature = ItemFeatures.getItemFeatures(itemClickSet,itemBuySet)

  }

}
