package com.daoke360.utils

import java.math.BigDecimal

/**
  * Created by ♠K on 2017-7-19.
  * QQ:272488352
  */
object NumberUtil {

  /**
    *
    * @param num   传入进来的需要四舍五入的数值
    * @param scale 保留几位小数
    * @return
    */
  def formatDouble(num: Double, scale: Int): Double = {
    val bd: BigDecimal = new BigDecimal(num)
    bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue()
  }


}
