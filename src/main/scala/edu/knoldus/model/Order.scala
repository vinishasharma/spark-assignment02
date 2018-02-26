package edu.knoldus.model

import java.util.Date

case class Order(private val timeStamp: Long, customerId: String, sales: Double) {
  val date = new Date(timeStamp * 1000)
}