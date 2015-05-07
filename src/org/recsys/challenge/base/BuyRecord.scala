package org.recsys.challenge.base

class BuyRecord(record:String) extends Serializable{
  private val ss = record.split(",")
  val sessionId = ss(0)
  val time = ss(1)
  val itemId = ss(2)
  val price = ss(3).toInt
  val quantity = ss(4).toInt
  override  def toString() = record
}
