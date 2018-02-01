package com.daoke360.utils

/**
  * Created by ♠K on 2017-7-19.
  * QQ:272488352
  */
object StringUtil {
  /**
    *
    * @param oldValue  session_count=0|1s_3s=0|4s_6s=0|7s_9s=58|10s_30s=0
    * @param delimiter |
    * @param fieldName session_count
    * @return
    */
  def getAccumulatorFieldValue(oldValue: String, delimiter: String, fieldName: String): String = {
    //{session_count=0,1s_3s=0,4s_6s=0...}
    val fields = oldValue.split(delimiter)
    for (field <- fields) {
      //{session_count,0}
      val kv = field.split("=")
      if(kv.length==2){
        val key = kv(0)
        val value = kv(1)
        if (key.equals(fieldName)) {
          return value
        }
      }
    }
    return null
  }

  /**
    *
    * @param oldValue  session_count=0|1s_3s=0|4s_6s=0|7s_9s=58|10s_30s=0
    * @param delimiter |
    * @param fieldName session_count
    * @param newValue  100
    */
  def setAccumulatorFieldValue(oldValue: String, delimiter: String, fieldName: String, newValue: String):String = {

    //{session_count=0,1s_3s=0,4s_6s=0...}
    val fields = oldValue.split(delimiter)
    for (i <- 0 until (fields.length)) {
      val field = fields(i)
      val kv = field.split("=")
      if(kv.length==2){
        val key = kv(0)
        if (key.equals(fieldName)) {
          val newKV = fieldName + "=" + newValue
          fields(i) = newKV
        }
      }
    }

    val sb = new StringBuffer()
    for (i <- 0 until (fields.length)) {
      sb.append(fields(i))
      if (i < fields.length - 1) {
        sb.append("|")
      }
    }
    sb.toString
  }
}
