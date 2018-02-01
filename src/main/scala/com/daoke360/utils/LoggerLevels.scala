package com.daoke360.utils

import kafka.utils.Logging
import org.apache.log4j.{Level, Logger}

object LoggerLevels extends Logging {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }

}