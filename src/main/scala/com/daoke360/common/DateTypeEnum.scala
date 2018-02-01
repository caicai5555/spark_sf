package com.daoke360.common

import com.daoke360.common.EventEnum.Value

/**
  * Created by ♠K on 2017-8-1.
  * QQ:272488352
  */
object DateTypeEnum  extends Enumeration{
  val year = Value(1, "year")
  val season = Value(2, "season")
  val month = Value(3, "month")
  val week = Value(4, "week")
  val day =Value(5, "day")
}
