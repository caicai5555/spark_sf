package com.daoke360.common

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object GlobalConstants {

  val HDFS_LOG_PATH = "hdfs_log_path"


  //habse 连接zk的主机
  val HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
  //hbase 连接zk端口
  val HBASE_ZOOKEEPER_PROPERTY = "hbase.zookeeper.property.clientPort"


  //任务运行参数标识
  val RUN_PARAMS_FLAG = "-d"
  //任务Id
  val RUN_TASK_ID = "taskId"

  /**
    * 默认值
    */
  val DEFAULT_VALUE: String = "unknown"
  /**
    * 任务的运行时间
    */
  val RUNNING_DATE_PARAMS: String = "RUNNING_DATE"
  /**
    * 一天中的毫秒数
    */
  val DAY_OF_MILLISECOND: Long = 24 * 60 * 60 * 1000
  /**
    *
    */
  val VALUE_OF_ALL: String = "all"
  /**
    * output_collect前缀
    */
  val OUTPUT_COLLECTOR_KEY_PREFIX: String = "collector_"
  /**
    * 默认的库
    */
  val JDBC_DEFAULT_DB: String = "report"

  /**
    * 默认批量提交mysql的数量
    */
  val JDBC_DEFAULT_BATCH_NUMBER: Int = 500

  /**
    * 每次提交到mysql的数据量 batch
    */
  val JDBC_BATCH_NUMBER: String = "mysql.%s.batch.number"

  /**
    * mysql 的驱动类
    */
  val JDBC_DRIVER: String = "jdbc.%s.driver"

  /**
    * mysql url
    */
  val JDBC_URL: String = "jdbc.%s.url"

  /**
    * mysql 用户名称
    */
  val JDBC_USER: String = "jdbc.%s.user"

  /**
    * mysql 用户密码
    */
  val JDBC_PASSWORD: String = "jdbc.%s.password"
  /**
    * 初始化连接池大小
    */
  val JDBC_DATASOURCE_SIZE = "jdbc.%s.datasource.size"

  //后台分析msql数据库
  val JDBC_BI = "bi"

  //线上业务库mysql的数据库
  val JDBC_ECS = "ecs"


  /**
    * spark任务相关的参数
    */
  val START_DATE = "startDate"
  val END_DATE = "endDate"


  /**
    * spark作业相关的参数
    */
  val FIELD_PARAM_SID = "sid"
  val FIELD_PARAM_UUID = "uid"
  val FIELD_PARAM_CLICK_GOODS_IDS = "clickGoodsIds"
  val FIELD_PARAM_STEP_LENGTH = "stepLength"
  val FIELD_PARAM_VISIT_LENGTH = "visitLength"
  val FIELD_CLICK_COUNT: String = "clickCount"
  val FIELD_CART_COUNT: String = "cartCount"
  val FIELD_PAY_COUNT: String = "payCount"
  val FIELD_CATEGORY_ID: String = "categoryid"


  /**
    * spark累加器相关的参数
    */
  val SESSION_COUNT: String = "session_count"

  val TIME_PERIOD_1s_3s: String = "1s_3s"
  val TIME_PERIOD_4s_6s: String = "4s_6s"
  val TIME_PERIOD_7s_9s: String = "7s_9s"
  val TIME_PERIOD_10s_30s: String = "10s_30s"
  val TIME_PERIOD_30s_60s: String = "30s_60s"
  val TIME_PERIOD_1m_3m: String = "1m_3m"
  val TIME_PERIOD_3m_10m: String = "3m_10m"
  val TIME_PERIOD_10m_30m: String = "10m_30m"
  val TIME_PERIOD_30m: String = "30m"


  val STEP_PERIOD_1_3: String = "1_3"
  val STEP_PERIOD_4_6: String = "4_6"
  val STEP_PERIOD_7_9: String = "7_9"
  val STEP_PERIOD_10_30: String = "10_30"
  val STEP_PERIOD_30_60: String = "30_60"
  val STEP_PERIOD_60: String = "60"


  /**
    * Spark-Streaming相关配置
    */
  val STREAMING_CHECKPOINT_PATH = "streaming.checkpoint.path"
  val STREAMING_STOP_PATH = "streaming.stop.path"
  val STREAMING_INTERVAL = "streaming.interval"

  val KAFKA_BROKER_LIST = "metadata.broker.list"
  val KAFKA_TOPIC = "kafka.topic"
  val KAFKA_OFFSET_RESET = "auto.offset.reset"
  val KAFKA_GROUP_ID = "group.id"

}
