package com.daoke360.conf

import java.util.Properties

import com.daoke360.common.GlobalConstants

/**
  * Created by ♠K on 2017-7-17.
  * QQ:272488352
  */
object ConfigurationManager {
  private val prop = new Properties()
  private val inputStream = ConfigurationManager.getClass.getClassLoader.getResourceAsStream("conf.properties")
  prop.load(inputStream)

  def getProperty(key: String) = {
    prop.getProperty(key)
  }

  def getInteger(key: String) = {
    prop.getProperty(key).toInt
  }


  /* def main(args: Array[String]): Unit = {
    println( getProperty(GlobalConstants.HBASE_ZOOKEEPER_QUORUM))
   }*/

}
