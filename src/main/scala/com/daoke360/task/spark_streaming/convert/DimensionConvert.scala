package com.daoke360.task.spark_streaming.convert

import java.sql.{Connection, Date, PreparedStatement, ResultSet, SQLException}

import com.daoke360.domain.{BaseDimension, DateDimension, LocationDimension}
import com.daoke360.jdbc.JdbcManager

/**
  * Created by ♠K on 2017-8-1.
  * QQ:272488352
  */
object DimensionConvert {

  private def setArgs(pstmt: PreparedStatement, dimension: BaseDimension): Unit = {
    if (dimension.isInstanceOf[DateDimension]) {
      val dateDimension = dimension.asInstanceOf[DateDimension]
      pstmt.setInt(1, dateDimension.year)
      pstmt.setInt(2, dateDimension.season)
      pstmt.setInt(3, dateDimension.month)
      pstmt.setInt(4, dateDimension.week)
      pstmt.setInt(5, dateDimension.day)
      pstmt.setDate(6, new Date(dateDimension.calender.getTime))
      pstmt.setString(7, dateDimension.dateType)
    } else if (dimension.isInstanceOf[LocationDimension]) {
      val locationDimension = dimension.asInstanceOf[LocationDimension]
      pstmt.setString(1, locationDimension.country)
      pstmt.setString(2, locationDimension.province)
      pstmt.setString(3, locationDimension.city)
    }
  }


  private def executeSql(sql: Array[String], dimension: BaseDimension, connection: Connection): Int = {
    var pstmt: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val querySql = sql(0)
      pstmt = connection.prepareStatement(querySql)
      //给sql语句赋值
      this.setArgs(pstmt, dimension)
      resultSet = pstmt.executeQuery()
      if (resultSet.next()) {
        resultSet.getInt(1)
      } else {
        val insertSql = sql(1)
        pstmt = connection.prepareStatement(insertSql, 1) //1 代表 RETURN_GENERATED_KEYS 即插入后将自动生成的id返回
        this.setArgs(pstmt, dimension)
        val count = pstmt.executeUpdate()
        if (count > 0) {
          resultSet = pstmt.getGeneratedKeys
          if (resultSet.next()) {
            resultSet.getInt(1)
          } else {
            throw new RuntimeException("从数据库中获取id失败")
          }
        } else {
          throw new RuntimeException("从数据库中获取id失败")
        }
      }
    } catch {
      case e: SQLException => {
        e.printStackTrace()
        throw new RuntimeException("从数据库中获取id失败")
      }
    } finally {
      if (resultSet != null) {
        resultSet.close()
      }
      if (pstmt != null) {
        pstmt.close()
      }
    }


  }


  private def buildLocationSql(): Array[String] = {
    Array[String](
      "select id from dimension_location where country=? and province=? and city=?",
      "insert into dimension_location(country,province,city)values(?,?,?)"
    )

  }

  private def buildDateSql(): Array[String] = {
    Array[String](
      "select id from dimension_date where `year`=? and season=? and `month`=? and `week`=? and `day`=? and calendar=? and type=?",
      "insert into dimension_date(`year`,season,`month`,`week`,`day`,calendar,type)values(?,?,?,?,?,?,?)"
    )
  }


  def getfindDimensionId(dimension: BaseDimension, connection: Connection): Int = {
    var sql: Array[String] = null
    if (dimension.isInstanceOf[DateDimension]) {
      sql = this.buildDateSql()

    } else if (dimension.isInstanceOf[LocationDimension]) {
      sql = this.buildLocationSql()
    }
    synchronized {
      this.executeSql(sql, dimension, connection)
    }
  }

}
