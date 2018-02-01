package com.daoke360.Dao

import java.sql.ResultSet

import com.daoke360.domain.DateDimension
import com.daoke360.jdbc.JdbcManager
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array

/**
  * Created by ♠K on 2017-8-1.
  * QQ:272488352
  */
object DateDimensionDao {

  def findId(dateDimension: DateDimension): Int = {
    var id = 0
    var sql = "select id from dimension_date where `year`=? and season=? and `month`=? and `week`=? and `day`=? and calendar=? and type=?"
    val params = Array[Any](
      dateDimension.year,
      dateDimension.season,
      dateDimension.month,
      dateDimension.week,
      dateDimension.day,
      dateDimension.calender,
      dateDimension.dateType
    )
    JdbcManager.executeQuery(sql, params, (resultSet: ResultSet) => {
      if (resultSet.next()) {
        id = resultSet.getInt("id")
      } else {
        insert(dateDimension)
        id = findId(dateDimension)
      }
    })
    id
  }


  def insert(dateDimension: DateDimension) = {
    val sql = "insert into dimension_date(`year`,season,`month`,`week`,`day`,calendar,type)values(?,?,?,?,?,?,?)"
    val params = Array[Any](
      dateDimension.year,
      dateDimension.season,
      dateDimension.month,
      dateDimension.week,
      dateDimension.day,
      dateDimension.calender,
      dateDimension.dateType
    )
    JdbcManager.executeUpdate(sql, params)
  }
}
