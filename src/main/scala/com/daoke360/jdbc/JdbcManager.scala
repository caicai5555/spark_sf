package com.daoke360.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.daoke360.common.GlobalConstants
import com.daoke360.conf.ConfigurationManager

import scala.collection.mutable.ListBuffer

/**
  * Created by ♠K on 2017-7-17.
  * QQ:272488352
  */
object JdbcManager {


  /**
    * 获取连接对象
    *
    * @return
    */
  def getConnection(): Connection = {
    try {
      val driver = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_DRIVER, GlobalConstants.JDBC_BI))
      val url = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_URL, GlobalConstants.JDBC_BI))
      val user = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_USER, GlobalConstants.JDBC_BI))
      val passwd = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_PASSWORD, GlobalConstants.JDBC_BI))
      Class.forName(driver)
      DriverManager.getConnection(url, user, passwd)
    } catch {
      case e: SQLException => throw new RuntimeException("获取连接对象失败")
    }
  }

  /**
    * 定义增删改方法
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    var result: Int = 0
    try {
      connection = getConnection()
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (i <- 0 until (params.length)) {
        pstmt.setObject(i + 1, params(i))
      }
      result = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (pstmt != null) pstmt.close()
        if (connection != null) connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    result
  }

  /**
    * 定义一个查询的方法
    */
  def executeQuery(sql: String, params: Array[Any], process: (ResultSet) => Unit) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      connection = getConnection()
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (i <- 0 until (params.length)) {
        pstmt.setObject(i + 1, params(i))
      }
      resultSet = pstmt.executeQuery()
      process(resultSet)
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (resultSet != null) resultSet.close()
        if (pstmt != null) pstmt.close()
        if (connection != null) connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  /**
    * 批量执行增删改
    */
  def executeBatch(sql: String, paramList: Array[Array[Any]]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    var result: Array[Int] = null
    try {
      connection = getConnection()
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramList) {
        for (i <- 0 until (params.length)) {
          pstmt.setObject(i + 1, params(i))
        }
        pstmt.addBatch()
      }
      synchronized {
        result = pstmt.executeBatch()
      }
      connection.commit()
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (pstmt != null) pstmt.close()
        if (connection != null) connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    result
  }


}
