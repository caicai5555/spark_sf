package com.daoke360.task.spark_rdd.etl.utils

import com.daoke360.common.GlobalConstants
import cz.mallat.uasparser.{OnlineUpdater, UASparser}

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object UserAgentUtil {
  private val uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream)


  def analysisUserAgent(userAgent: String): UserAgentInfo = {
    val agentInfo = uaSparser.parse(userAgent)
    new UserAgentInfo(agentInfo.getUaFamily, agentInfo.getBrowserVersionInfo, agentInfo.getOsFamily, agentInfo.getOsName)
  }
}

class UserAgentInfo(var browserName: String = GlobalConstants.DEFAULT_VALUE,
                    var browserVersion: String = GlobalConstants.DEFAULT_VALUE,
                    var osName: String,
                    var osVersion: String){

  override def toString = s"UserAgentInfo(browserName=$browserName, browserVersion=$browserVersion, osName=$osName, osVersion=$osVersion)"
}