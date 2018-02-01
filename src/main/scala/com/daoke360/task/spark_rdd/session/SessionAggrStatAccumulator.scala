package com.daoke360.task.spark_rdd.session

import com.daoke360.common.GlobalConstants
import com.daoke360.utils.StringUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
  * Created by ♠K on 2017-7-19.
  * QQ:272488352
  */
object SessionAggrStatAccumulator extends AccumulatorParam[String] {

  /**
    * 累加器的初始值
    *
    * @param initialValue
    * @return
    */
  override def zero(initialValue: String): String = {
    //session_count=0|1s_3s=0|4s_6s=0|7s_9s=58|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
      GlobalConstants.SESSION_COUNT + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_1s_3s + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_4s_6s + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_7s_9s + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_10s_30s + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_30s_60s + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_1m_3m + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_3m_10m + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_10m_30m + "=" + 0 + "|" +
      GlobalConstants.TIME_PERIOD_30m + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_1_3 + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_4_6 + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_7_9 + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_10_30 + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_30_60 + "=" + 0 + "|" +
      GlobalConstants.STEP_PERIOD_60 + "=" + 0
  }

  /**
    * 定义如何进行累加
    *
    * @param v1 累加器里面存放的值
    * @param v2 新添加进来的值
    * @return 返回累加之后的结果
    */
  override def addInPlace(v1: String, v2: String): String = {
    //累加器最后还会做一次累加，此时v1 传入进来的是 "",所以需要在此处做一个判断
    if (v1 == "") {
      v2
    } else {
      val oldValue = StringUtil.getAccumulatorFieldValue(v1, "\\|", v2)
      var newValue = 0
      if (oldValue != null) {
        newValue = oldValue.toInt + 1
      }
      StringUtil.setAccumulatorFieldValue(v1, "\\|", v2, newValue.toString)
    }
  }

}
