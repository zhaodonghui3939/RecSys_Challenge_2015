package org.recsys.challenge

import org.apache.spark.SparkContext
import org.recsys.challenge.Sample.SampleBase
import org.recsys.challenge.feature.{FeatureEngineering}
import org.recsys.challenge.model.Training

object RecSysMain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val clicks = sc.textFile("/data/recsys2015/yoochoose-clicks.dat").cache()
    val buys = sc.textFile("/data/recsys2015/yoochoose-buys.dat").cache()
    val test = sc.textFile("/data/recsys2015/yoochoose-test.dat").cache()
    val fe = new FeatureEngineering(clicks,buys,test)

    val trainingFeatures = fe.getTraningFeatures.cache()
    val sample = SampleBase.globalSample(trainingFeatures,8).cache()

    val model = Training.rf(sample)

    val predictFeatures = fe.getPredictFeatures.cache()



  }

}
