package com.daoke360.domain

import java.util.{Calendar, Date}

import com.daoke360.common.DateTypeEnum
import com.daoke360.utils.TimeUtil


/**
  * Created by ♠K on 2017-7-31.
  * QQ:272488352
  */
class DateDimension(var year: Int, var season: Int, var month: Int, var week: Int, var day: Int, var calender: Date, var dateType: String) extends BaseDimension {
  var id: Int = 0

  def canEqual(other: Any): Boolean = other.isInstanceOf[DateDimension]

  override def equals(other: Any): Boolean = other match {
    case that: DateDimension =>
      (that canEqual this) &&
        this.id == that.id &&
        this.year == that.year &&
        this.season == that.season &&
        this.month == that.month &&
        this.week == that.week &&
        this.day == that.day &&
        this.calender.getTime == that.calender.getTime &&
        this.dateType.equals(that.dateType)
    case _ => false
  }
}

object DateDimension {

  def buildDate(longTime: Long, dateType: DateTypeEnum.Value): DateDimension = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(longTime)
    val year = TimeUtil.getDateInfo(longTime, DateTypeEnum.year)
    val season = TimeUtil.getDateInfo(longTime, DateTypeEnum.season)
    val month = TimeUtil.getDateInfo(longTime, DateTypeEnum.month)
    var week = TimeUtil.getDateInfo(longTime, DateTypeEnum.week)
    val day = TimeUtil.getDateInfo(longTime, DateTypeEnum.day)
    if (DateTypeEnum.year.equals(dateType)) {
      calendar.set(year, 0, 1)
      new DateDimension(year, 0, 0, 0, 0, calendar.getTime, dateType.toString)
    } else if (DateTypeEnum.season.equals(dateType)) {
      val month = season * 3 - 2
      calendar.set(year, month - 1, 1)
      new DateDimension(year, season, 0, 0, 0, calendar.getTime, dateType.toString)
    } else if (DateTypeEnum.month.equals(dateType)) {
      calendar.set(year, month - 1, 1)
      new DateDimension(year, season, month, 0, 0, calendar.getTime, dateType.toString)
    } else if (DateTypeEnum.week.equals(dateType)) {
      val firstDayOfWeekTime = TimeUtil.getFirstDayOfWeek(longTime)
      week = TimeUtil.getDateInfo(firstDayOfWeekTime, DateTypeEnum.week)
      if (month == 12 && week == 1) {
        week = 53
      }
      new DateDimension(year, season, month, week, 0, new Date(firstDayOfWeekTime), dateType.toString)
    } else if (DateTypeEnum.day.equals(dateType)) {
      if (month == 12 && week == 1) {
        week = 53
      }
      calendar.set(year, month - 1, day)
      new DateDimension(year, season, month, week, day, calendar.getTime, dateType.toString)
    } else {
      throw new RuntimeException("获取时间维度失败")
    }
  }

}
