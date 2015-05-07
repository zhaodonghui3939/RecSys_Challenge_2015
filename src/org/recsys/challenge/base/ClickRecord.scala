package org.recsys.challenge.base

class ClickRecord(record:String) extends Serializable{
  private val ss = record.split(",")
  val sessionId = ss(0)
  val time = ss(1)
  val itemId = ss(2)
  val category = ss(3)
  override def toString() = record
}
