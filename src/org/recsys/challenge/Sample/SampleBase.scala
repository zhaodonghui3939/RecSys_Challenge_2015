package org.recsys.challenge.Sample
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
object SampleBase {
  //正负样本采样比例
  def globalSample(data:RDD[(String,LabeledPoint)],rate:Double): RDD[LabeledPoint]= {
    val postive = data.filter(_._2.label == 1.0).count()
    val negtive = data.filter(_._2.label != 1.0).count()
    val percent  = Math.min(1,postive * rate / negtive)
    data.filter(_._2.label == 1.0).map(_._2).union(data.filter(_._2.label != 1.0).map(_._2).sample(false,percent))
  }
}