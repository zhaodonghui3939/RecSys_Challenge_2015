package org.recsys.challenge.feature

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.recsys.challenge.base.ClickRecord

object SessionItemFeatures extends Serializable{

  //将时间转化为秒
  private def stringToLong(time:String):Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val t = time.substring(0,time.length - 5).replace("T"," ")
    (formatter.parse(t).getTime - formatter.parse("2014-04-01 00:00:00").getTime)/1000
  }
  def getSessionItemClickFeatures(itemsClick: RDD[(String, Array[ClickRecord])]): RDD[(String, Array[Double])] = {
    itemsClick.map{
      case(item,records) => {

      }
    }

  }

}
