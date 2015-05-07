package org.recsys.challenge.feature

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.recsys.challenge.base.ClickRecord

object SessionFeatures extends Serializable{

  //将时间转化为秒
  private def stringToLong(time:String):Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val t = time.substring(0,time.length - 5).replace("T"," ")
    (formatter.parse(t).getTime - formatter.parse("2014-04-01 00:00:00").getTime)/1000
  }

  def getSessionFeatures(session:RDD[(String,Array[ClickRecord])]) = {
    session.map{
      case (sessionId,records) => {
        val click_sum = records.length                             //该session中点击的总次数
        val item_num = records.map(_.itemId).distinct.length       //不同item的数量
        val time_inteval = {                                       //时间间隔
          val records_sorted = records.map(line => stringToLong(line.time)).sorted
          (records_sorted(records_sorted.length - 1) - records_sorted(0))
        }
        val click_time = time_inteval / click_sum.toDouble         //点击总次数除以时间间隔
        val click_item = click_sum.toDouble / item_num             //点击总次数除以商品种类数
        val category_num = records.map(_.category).distinct.length //种类的总数目
        val promotion = records.map(line => (line.itemId,line.category)).distinct.map(_._2).filter(_.equals("S")).length //促销的商品数目
        val features = Array(click_sum,item_num,time_inteval,click_time,click_item,category_num,promotion)
        (sessionId,features)
      }
    }
  }
}
