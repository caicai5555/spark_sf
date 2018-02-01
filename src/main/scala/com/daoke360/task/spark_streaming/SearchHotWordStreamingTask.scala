package com.daoke360.task.spark_streaming

import java.util.Properties

import com.daoke360.common.{EventLogConstants, GlobalConstants}
import com.daoke360.conf.ConfigurationManager
import com.daoke360.domain.{DateDimension, LocationDimension}
import com.daoke360.task.spark_rdd.etl.utils.{IPRule, LoggerUtil}
import com.daoke360.task.spark_streaming.kafkamanager.KafkaManager
import com.daoke360.utils.{LoggerLevels, TimeUtil}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ♠K on 2017-8-4.
  * QQ:272488352
  */
object SearchHotWordStreamingTask {
  val checkPointPath = String.format(StreamingConstants.checkPointPath, "SearchHotWordStreamingTask" + System.currentTimeMillis())

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("").setMaster("local[8]")
    //设置使用的序列化处理类
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册自定义类交给KryoSerializer序列化处理类进行序列化
    sparkConf.registerKryoClasses(Array(classOf[IPRule], classOf[DateDimension], classOf[LocationDimension]))
    val sc = new SparkContext(sparkConf)
    val ipRulesBroadcast = brodCastIpRules(sc)
    //使用高可靠的driver方式创建ssc
    val ssc = StreamingContext.getOrCreate(checkPointPath, () => createStreamingContext(sc, ipRulesBroadcast))
    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(sc: SparkContext, ipRulesBroadcast: Broadcast[Array[IPRule]]): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(StreamingConstants.batchInterver))
    ssc.checkpoint(checkPointPath)
    val kafkaParams = StreamingConstants.kafkaParams
    val topicSet = Set(StreamingConstants.kafkaTopic)

    val kafkaManager = new KafkaManager(kafkaParams, topicSet)
    //创建输入的dstream
    val commonDStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc)
      .map(logText => {
        LoggerUtil.handleLog(logText, ipRulesBroadcast.value)
      }).filter(x => x.size != 0 && x.contains(EventLogConstants.LOG_COLUMN_NAME_KEYWORD)).map(clientInfoMap => {
      val serverTime = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)
      val country = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_COUNTRY)
      val province = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_PROVINCE)
      val city = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_CITY)
      val keyword = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_KEYWORD)
      ((TimeUtil.parseLongToString(serverTime.toLong, "yyyy-MM-dd"), country, province, city, keyword), 1)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60 * 60), Seconds(4)).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val searchInfoRDD = rdd.map(t2 => SearchInfo(t2._1._1, t2._1._2, t2._1._3, t2._1._4, t2._1._5, t2._2))
        val sparkSession = SparkSession.builder().getOrCreate()
        import sparkSession.sqlContext.implicits._
        searchInfoRDD.toDS().createOrReplaceTempView("searchInfoView")

        val url = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_URL, GlobalConstants.JDBC_BI))
        val user = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_USER, GlobalConstants.JDBC_BI))
        val password = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_PASSWORD, GlobalConstants.JDBC_BI))
        val prop = new Properties()
        prop.put("user", user)
        prop.put("password", password)
        sparkSession.sql(
          """
            |select rank,day,country,province,city,keyword,count
            |from(
            |       select  row_number()over(partition by day,country,province order by count desc) rank,
            |       day,country,province,city,keyword,count from searchInfoView
            | )tmp  where rank <=3
          """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(url, "search_hot_keyword", prop)
      }
      kafkaManager.updateOffsetToZK()
    })
    ssc
  }

  def brodCastIpRules(sc: SparkContext) = {
    val iPRules = sc.textFile("/spark_sf_project/resource/ip.data").map(line => {
      //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val fields = line.split("\\|")
      val startIP = fields(2).toLong
      val endIP = fields(3).toLong
      val country = fields(5)
      val province = fields(6)
      val city = fields(7)
      IPRule(startIP, endIP, country, province, city)
    }).collect()
    val ipRulesBroadcast: Broadcast[Array[IPRule]] = sc.broadcast[Array[IPRule]](iPRules)
    ipRulesBroadcast
  }

  case class SearchInfo(var day: String, var country: String, var province: String, var city: String, var keyword: String, var count: Int)

}
