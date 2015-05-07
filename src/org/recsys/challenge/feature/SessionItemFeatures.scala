package org.recsys.challenge.feature

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.recsys.challenge.base.ClickRecord
import java.util.{Calendar,Date}

object SessionItemFeatures extends Serializable{

  //将时间转化为秒
  private def stringToLong(time:String):Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val t = time.substring(0,time.length - 5).replace("T"," ")
    (formatter.parse(t).getTime - formatter.parse("2014-04-01 00:00:00").getTime)/1000
  }
  private def stringToDate(time:String):Calendar = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val t = time.substring(0,time.length - 5).replace("T"," ")
    val cal = Calendar.getInstance()
    cal.setTime(formatter.parse(t))
    cal
  }
  def getSessionItemClickFeatures(itemsClick: RDD[(String, Array[ClickRecord])]): RDD[(String, Array[Double])] = {
    itemsClick.map{
      case(session_item,records) => {
        val click_sum = records.length
        val time_inteval = {
          val records_sorted = records.map(line => stringToLong(line.time)).sorted
          (records_sorted(records_sorted.length - 1) - records_sorted(0))
        }
        val click_time = time_inteval / click_sum.toDouble
        val category_flag = {
          val set = Set("0","1","2","3","4","5","6","7","8","9","10","11","12")
          if(set.contains(records(0).category)) records(0).category.toInt
          else if(records(0).category.equals("S")) 14
          else 15
        }
        val month = stringToDate(records(0).time).get(Calendar.MONTH) + 1
        val month_day = stringToDate(records(0).time).get(Calendar.DAY_OF_MONTH)
        val week_day = stringToDate(records(0).time).get(Calendar.DAY_OF_WEEK)
        val features = Array(click_sum,time_inteval,click_time,category_flag,month,month_day,week_day)
        (session_item,features)
      }
    }

  }

}
