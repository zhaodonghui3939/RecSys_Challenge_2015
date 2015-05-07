package org.recsys.challenge.feature

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.recsys.challenge.base.{BuyRecord, ClickRecord}

object ItemFeatures extends Serializable {

  private def stringToLong(time: String): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val t = time.substring(0, time.length - 5).replace("T", " ")
    (formatter.parse(t).getTime - formatter.parse("2014-04-01 00:00:00").getTime) / 1000
  }

  private def getItemClickFeatures(itemsClick: RDD[(String, Array[ClickRecord])]): RDD[(String, Array[Double])] = {
    itemsClick.map {
      case (itemId, records) => {
        val click_sum = records.length //点击总次数
        val session_sum = records.map(_.sessionId).distinct.length //session的总数目
        val time_inteval = {
          val record_sorted = records.map(line => stringToLong(line.time)).sorted
          (record_sorted(record_sorted.length - 1) - record_sorted(0)) / 60
        }
        val click_session = click_sum.toDouble / session_sum //点击总次数除以session总数
        val features = Array(click_sum, session_sum, time_inteval, click_session)
        (itemId, features)
      }
    }
  }

  private def getItemBuyFeatures(itemsBuy: RDD[(String, Array[BuyRecord])]): RDD[(String, Array[Double])] = {
    itemsBuy.map {
      case (itemId, records) => {
        val buy_sum = records.map(_.quantity).sum
        val buy_time_sum = records.length
        val buy_session_sum = records.map(_.sessionId).distinct.length
        val buy_price = records.map(_.price).sum / records.length
        val features = Array(buy_sum.toDouble, buy_time_sum, buy_session_sum, buy_price)
        (itemId, features)
      }
    }
  }

  def getItemFeatures(itemsClick: RDD[(String, Array[ClickRecord])], itemsBuy: RDD[(String, Array[BuyRecord])]): RDD[(String, Array[Double])] = {
    val item_click_features = getItemClickFeatures(itemsClick)
    val item_buy_features = getItemBuyFeatures(itemsBuy)
    item_click_features.leftOuterJoin(item_buy_features).map {
      case (itemId, (clickFeatures, buyFeatures)) => {
        buyFeatures match {
          case Some(x) => (itemId, clickFeatures ++ x)
          case None => (itemId, clickFeatures ++ Array(0.0, 0.0, 0.0, 0.0))
        }
      }
    }
  }
}
