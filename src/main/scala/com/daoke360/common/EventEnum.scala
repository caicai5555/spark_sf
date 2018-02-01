package com.daoke360.common

/**
  * Created by ♠K on 2017-7-12.
  * QQ:272488352
  */
object EventEnum extends Enumeration {

  val launch = Value(1, "e_l")
  val pageView = Value(2, "e_pv")
  val submitOrder = Value(3, "e_sod")
  val search = Value(4, "e_s")
  val addToCart =Value(5, "e_cart")
  val eventDuration =Value(6, "e_e")

  def valueOfName(eventName: String) = {
    var eventEnum: Value = null
    for (value <- EventEnum.values) {
      if (value.toString.equals(eventName)) {
        eventEnum = EventEnum.withName(eventName)
      }
    }
    eventEnum
  }
}
