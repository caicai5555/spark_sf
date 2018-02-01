package com.daoke360.task.spark_streaming

import com.daoke360.common.GlobalConstants
import com.daoke360.conf.ConfigurationManager

/**
  * Created by ♠K on 2017-7-31.
  * QQ:272488352
  */
object StreamingConstants {
  //获取生成Batch的时间间隔
  val batchInterver = ConfigurationManager.getInteger(GlobalConstants.STREAMING_INTERVAL)
  //checkpoint目录
  val checkPointPath = ConfigurationManager.getProperty(GlobalConstants.STREAMING_CHECKPOINT_PATH)
  //streaming 停止目录
  val stopPath = ConfigurationManager.getProperty(GlobalConstants.STREAMING_STOP_PATH)
  //kafka集群节点
  val kafkaBrokerList = ConfigurationManager.getProperty(GlobalConstants.KAFKA_BROKER_LIST)
  //kafka的topic
  val kafkaTopic = ConfigurationManager.getProperty(GlobalConstants.KAFKA_TOPIC)
  //消费的起始位置
  val kafkaAutoOffsetReset = ConfigurationManager.getProperty(GlobalConstants.KAFKA_OFFSET_RESET)
  //消费者组
  val kafkaGroupId = ConfigurationManager.getProperty(GlobalConstants.KAFKA_GROUP_ID)

  //kafka相关参数
  val kafkaParams = Map(
    GlobalConstants.KAFKA_BROKER_LIST -> kafkaBrokerList,
    GlobalConstants.KAFKA_GROUP_ID -> kafkaGroupId,
    GlobalConstants.KAFKA_OFFSET_RESET -> kafkaAutoOffsetReset
  )

}
