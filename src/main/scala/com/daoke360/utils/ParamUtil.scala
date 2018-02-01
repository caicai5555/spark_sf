package com.daoke360.utils

import com.alibaba.fastjson.JSONObject
import com.google.protobuf.TextFormat.ParseException

/**
  * Created by ♠K on 2017-7-17.
  * QQ:272488352
  */
object ParamUtil {

  def isValidateNumber(stringNumber: String): Boolean = {
    var flag = true
    try {
      stringNumber.toInt
    } catch {
      case e: Exception => flag = false
    }
    flag
  }


  def getParam(key: String, param: JSONObject): Any = {
    param.get(key)
  }

  def main(args: Array[String]): Unit = {
    var clickGoodsIds = "1,2,3,"
    println(clickGoodsIds.substring(0, clickGoodsIds.length - 1))
  }

}
