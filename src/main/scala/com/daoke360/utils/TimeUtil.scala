package com.daoke360.utils

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.daoke360.common.DateTypeEnum

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object TimeUtil {
  private val DEFAULT_FORMAT = "yyyy-MM-dd"
  private val sdf = new SimpleDateFormat(DEFAULT_FORMAT)


  /**
    * 获取时间戳所属的周的第一天
    *
    */
  def getFirstDayOfWeek(longTime: Long) = {
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    calendar.set(Calendar.DAY_OF_WEEK, 1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.getTimeInMillis
  }

  /**
    * 获取时间单位（年，季度，月，周，日）
    *
    * @param longTime
    * @param dateType
    * @return
    */
  def getDateInfo(longTime: Long, dateType: DateTypeEnum.Value): Int = {
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    if (DateTypeEnum.year.equals(dateType)) {
      calendar.get(Calendar.YEAR)
    } else if (DateTypeEnum.season.equals(dateType)) {
      val month = calendar.get(Calendar.MONTH) + 1
      if (month % 3 == 0) {
        month / 3
      } else {
        (month / 3) + 1
      }
    } else if (DateTypeEnum.month.equals(dateType)) {
      calendar.get(Calendar.MONTH) + 1
    } else if (DateTypeEnum.week.equals(dateType)) {
      calendar.get(Calendar.WEEK_OF_YEAR)
    } else if (DateTypeEnum.day.equals(dateType)) {
      calendar.get(Calendar.DAY_OF_MONTH)
    } else {
      throw new RuntimeException("指定的时间单位不存在")
    }

  }


  /**
    * 获取昨天的日期
    */
  def getYesterday: String = {
    val calendar = Calendar.getInstance
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    sdf.format(calendar.getTime)
  }


  /**
    * 验证输入的日期参数是否是 yyyy-MM-dd
    *
    * @param date
    * @return
    */
  def isValiDateRunningDate(date: String): Boolean = {
    //http://www.regexlib.com/Search.aspx?k=time
    val regex = "([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(date)
    matcher.matches
  }

  /**
    * 将传入进来的double 类型的字符串时间，转换成long类型的时间
    */
  def parNginxServerTime2Long(longTime: String): Long = {
    val date = parseNginxServerTime2Date(longTime)
    if (date == null) -1L
    else date.getTime
  }

  /**
    * 将nginx的时间解析成日期对象 Date
    */
  def parseNginxServerTime2Date(longTime: String): Date = {
    val time = ((longTime.trim).toDouble * 1000).toLong
    val cd = Calendar.getInstance
    cd.setTimeInMillis(time)
    cd.getTime
  }


  /**
    * 将long类型的日期转换成指定格式的字符串日期
    */
  def parseLongToString(date: Long, pattern: String): String = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(date)
    sdf.applyPattern(pattern)
    sdf.format(calendar.getTime)
  }

  /**
    * 将日期转换成long类型
    */
  def parseStringToLong(strDate: String, parttern: String = DEFAULT_FORMAT): Long = {
    //重组日期格式化对象的日期格式
    sdf.applyPattern(parttern)
    val date = sdf.parse(strDate)
    date.getTime
  }


}
