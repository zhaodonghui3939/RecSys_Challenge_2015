package org.recsys.challenge

import org.apache.spark.SparkContext
import org.recsys.challenge.sample.SampleBase
import org.recsys.challenge.feature.{FeatureEngineering}
import org.recsys.challenge.model.{Predict, Training}

object RecSysMain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val clicks = sc.textFile("/data/recsys2015/yoochoose-clicks.dat")
    val buys = sc.textFile("/data/recsys2015/yoochoose-buys.dat")
    val test = sc.textFile("/data/recsys2015/yoochoose-test.dat")
    //特征工程和采样训练
    val fe = new FeatureEngineering(clicks,buys,test)
    val trainingFeatures = fe.getTraningFeatures.cache()
    val sample = SampleBase.globalSample(trainingFeatures,1).cache()
    val model = Training.rf(sample)

    //线下测试准略率
    val trainingResults = Predict.predictRF(trainingFeatures,model,0.6).cache()

    //开始预测
    val predictFeatures = fe.getPredictFeatures.cache()
    val predictResults = Predict.predictRF(predictFeatures,model,0.6);
    //结果输出
    val output = predictResults.map(_._1).map{
      case sessionItem => {
        val session = sessionItem.split("_")(0)
        val item = sessionItem.split("_")(1)
        (session,item)
      }
    }.reduceByKey(_+","+_).map{
      case (session,items) => {
        session+";"+items
      }
    }.cache()

    output.saveAsTextFile("/data/recsys2015/0507/rf")
  }

}
