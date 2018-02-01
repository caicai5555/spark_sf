package com.daoke360.task.spark_rdd.etl.utils

import java.net.URLDecoder

import com.daoke360.common.{EventLogConstants, GlobalConstants}
import com.daoke360.utils.{ParamUtil, TimeUtil}
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object LoggerUtil {


  /**
    * 解析每条日志，将key和value封装到Map<String,String>
    *
    * @param logText 完整的日志 192.168.80.8 1492136603.400 mini1 /log.gif?en=e_pv&ver=1&pl=website
    * @return Map<String,String>
    */
  def handleLog(logText: String, iPRules: Array[IPRule]): mutable.HashMap[String, String] = {
    val clientInfo = new mutable.HashMap[String, String]
    if (StringUtils.isNotBlank(logText)) {
      val fields = logText.trim.split(EventLogConstants.LOG_SEPARATOR)
      if (fields.length == 4) {
        //客户端ip
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, fields(0).trim)
        //服务器时间
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(TimeUtil.parNginxServerTime2Long(fields(1).trim)))
        val requestBody = fields(3).trim
        // 解析客户端发送过来的请求体参数
        handleRequestBody(requestBody, clientInfo)
        //解析用户代理信息
        handleUserAgent(clientInfo)
        //解析有用地域信息
        handleIp(clientInfo, iPRules)
        //解析搜索关键词
        handleSearchKeyword(clientInfo)
      }
      //移除掉浏览器用户代理信息
      clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT)
    }

    clientInfo
  }

  /**
    * 解析用户地域信息
    */
  private def handleIp(clientInfo: mutable.HashMap[String, String], iPRules: Array[IPRule]) {
    if (clientInfo.contains(EventLogConstants.LOG_COLUMN_NAME_IP)) {
      val regionInfo = IPUtil.analysisIP(clientInfo(EventLogConstants.LOG_COLUMN_NAME_IP), iPRules)
      if (regionInfo != null) {
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, regionInfo.country)
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, regionInfo.province)
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, regionInfo.city)
      }
    }
  }

  /**
    * 解析用户代理信息
    */
  private def handleUserAgent(clientInfo: mutable.HashMap[String, String]) {
    if (clientInfo.contains(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT)) {
      val userAgentInfo = UserAgentUtil.analysisUserAgent(clientInfo(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT))
      if (userAgentInfo != null) {
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, userAgentInfo.browserName)
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, userAgentInfo.browserVersion)
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, userAgentInfo.osName)
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, userAgentInfo.osVersion)
      }
    }
  }

  /**
    * 解析客户端发送过来的请求体参数
    */
  private def handleRequestBody(requestBody: String, clientInfo: mutable.HashMap[String, String]) {
    if (StringUtils.isNotBlank(requestBody)) {
      val index = requestBody.indexOf("?")
      //{"en=e_l","ver=1",.....}
      val params = requestBody.substring(index + 1).split(EventLogConstants.LOG_PARAM_SEPARATOR)
      for (param <- params) {
        if (StringUtils.isNotBlank(param)) {
          val kv = param.trim.split("=")
          var key = kv(0)
          var value = URLDecoder.decode(kv(1), "utf-8")
          if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
            clientInfo.put(key, value)
            //用户点击了品类 http://www.daoke360.cn/category.php?id=683
            if (key.equals(EventLogConstants.LOG_COLUMN_NAME_URL) && value.contains(EventLogConstants.LOG_CATEGORY_FLAG)) {
              handleCategoryAndGoods(value, EventLogConstants.LOG_CATEGORY_FLAG, clientInfo)
            }
            //用户点击了具体的商品 http://www.daoke360.cn/goods.php?id=287
            if (key.equals(EventLogConstants.LOG_COLUMN_NAME_URL) && value.contains(EventLogConstants.LOG_GOODS_FLAG)) {
              handleCategoryAndGoods(value, EventLogConstants.LOG_COLUMN_NAME_GOODS, clientInfo)
            }
          }
        }
      }
    }
  }

  //http://www.daoke360.cn/category.php?id=683&a
  def handleCategoryAndGoods(url: String, key: String, clientInfo: mutable.HashMap[String, String]): Unit = {
    val startIndex = url.indexOf(EventLogConstants.LOG_IDS_FLAG)
    if (startIndex > 0) {
      var endIndex = url.indexOf("&", startIndex)
      if (endIndex == -1) {
        endIndex = url.length
      }
      val value = url.substring(startIndex + EventLogConstants.LOG_IDS_FLAG.length, endIndex)
      if (ParamUtil.isValidateNumber(value)) {
        clientInfo.put(key, value)
      }
    }
  }


  def handleSearchKeyword(clientInfo: mutable.HashMap[String, String]) = {

    if (clientInfo.contains(EventLogConstants.LOG_COLUMN_NAME_URL)
      && clientInfo(EventLogConstants.LOG_COLUMN_NAME_URL).contains(EventLogConstants.LOG_SEARCH_FLAG)
      && clientInfo.contains(EventLogConstants.LOG_COLUMN_NAME_TITLE)) {
      val keyword = clientInfo(EventLogConstants.LOG_COLUMN_NAME_TITLE).split("_")(1)
      if (StringUtils.isNotBlank(keyword)) {
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_KEYWORD, keyword)
      }
    }

  }
}
