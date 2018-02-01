package com.daoke360.task.spark_rdd.etl.utils

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.daoke360.common.GlobalConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object IPUtil {
 /* private val IPRules = new ArrayBuffer[IPRule]()
  private val inputStream: InputStream = IPUtil.getClass.getClassLoader.getResourceAsStream("ip.data")
  private val reader: BufferedReader = new BufferedReader(new InputStreamReader(inputStream))
  breakable({
    while (true) {
      val line: String = reader.readLine
      if (StringUtils.isBlank(line)) {
        break()
      } else {

        val fields: Array[String] = line.split("\\|")
        //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
        val startIP: Long = fields(2).toLong
        val endIP: Long = fields(3).toLong
        val country: String = fields(5)
        val province: String = fields(6)
        val city: String = fields(7)
        IPRules.append(new IPRule(startIP, endIP, country, province, city))
      }
    }
  })*/

  def analysisIP(ip: String,IPRules:Array[IPRule]): RegionInfo = {
    val info = new RegionInfo
    if (!ip.contains("192.168")) {
      // 将ip地址转换成完整的十进制
      val numIP = ip2Long(ip)
      val index = binarySearch(numIP,IPRules)
      if (index != -1) {
        val ipRule = IPRules(index)
        info.country=ipRule.country
        info.province=ipRule.province
        info.city=ipRule.city
      }
    }
    info
  }




  private def binarySearch(numIP: Long,IPRules:Array[IPRule]) = {
    var min = 0
    var max = IPRules.size - 1
    var middle = -1
    breakable({
      while (min <= max) {
        middle = (min + max) / 2
        val ipRule = IPRules(middle)
        if (numIP >= ipRule.startIP && numIP <= ipRule.endIP) break()
        else if (numIP < ipRule.startIP) max = middle - 1
        else if (numIP > ipRule.endIP) min = middle + 1
      }
    })
    middle
  }


  private def ip2Long(ip: String): Long = {
    val fields = ip.split("[.]")
    var numIP = 0L
    var i = 0
    for (i <- 0 until (fields.length)) {
      numIP = numIP << 8 | fields(i).toLong
    }
    numIP
  }
}

case  class IPRule(var startIP: Long, var endIP: Long, var country: String, var province: String, var city: String)

class RegionInfo(var country: String = GlobalConstants.DEFAULT_VALUE,
                 var province: String = GlobalConstants.DEFAULT_VALUE,
                 var city: String = GlobalConstants.DEFAULT_VALUE){

  override def toString = s"RegionInfo(country=$country, province=$province, city=$city)"
}