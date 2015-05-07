package org.recsys.challenge.sample
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
object SampleBase {
  //正负样本采样比例
  def globalSample(data:RDD[(String,LabeledPoint)],times:Double): RDD[LabeledPoint]= {
    val postive = data.filter(_._2.label == 1.0).count() //正样本个数
    val negtive = data.filter(_._2.label != 1.0).count() //负样本个数
    val percent  = Math.min(1,postive * times / negtive)
    data.filter(_._2.label == 1.0).map(_._2).union(data.filter(_._2.label != 1.0).map(_._2).sample(false,percent))
  }
}
