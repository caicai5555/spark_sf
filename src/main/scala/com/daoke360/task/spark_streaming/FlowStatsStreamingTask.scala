package com.daoke360.task.spark_streaming

import java.util.Date

import com.daoke360.Dao.StatsLocationFlowDao
import com.daoke360.common.{DateTypeEnum, EventEnum, EventLogConstants, GlobalConstants}
import com.daoke360.domain.{DateDimension, LocationDimension, StatsLocationFlow}
import com.daoke360.jdbc.JdbcManager
import com.daoke360.task.spark_rdd.etl.utils.{IPRule, LoggerUtil}
import com.daoke360.task.spark_streaming.convert.DimensionConvert
import com.daoke360.task.spark_streaming.kafkamanager.KafkaManager
import com.daoke360.utils.{LoggerLevels, TimeUtil}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by ♠K on 2017-7-31.
  * QQ:272488352
  */
object FlowStatsStreamingTask {
  val checkPointPath = String.format(StreamingConstants.checkPointPath, "FlowStatsStreamingTask" + System.currentTimeMillis())

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
      }).filter(x => x.size != 0).map(clientInfoMap => {
      val eventName = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)
      val serverTime = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)
      val country = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_COUNTRY)
      val province = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_PROVINCE)
      val city = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_CITY)
      val uuid = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_UUID)
      val sid = clientInfoMap(EventLogConstants.LOG_COLUMN_NAME_SID)
      (serverTime, eventName, country, province, city, uuid, sid)
    }).flatMap(t7 => {
      LocationDimension.buildList(t7._3, t7._4, t7._5).map(locationDimension => {
        (
          (TimeUtil.parseLongToString(t7._1.toLong, "yyyy-MM-dd"), locationDimension.country, locationDimension.province, locationDimension.city),
          (t7._2, t7._6, t7._7)
        )
      })

    }).updateStateByKey((it: Iterator[((String, String, String, String), Seq[(String, String, String)], Option[ArrayBuffer[Any]])]) => {
      it.map(x => {
        val resutSet = x._3.getOrElse(ArrayBuffer[Any](new mutable.HashSet[String](), new mutable.HashMap[String, Int], 0, 0))
        val uuidSet = resutSet(0).asInstanceOf[mutable.HashSet[String]]
        val sessionHashMap = resutSet(1).asInstanceOf[mutable.HashMap[String, Int]]
        var nu = resutSet(2).asInstanceOf[Int]
        var pv = resutSet(3).asInstanceOf[Int]
        //t3 ==>(eventName,uuid,sid)
        x._2.foreach(t3 => {
          uuidSet.add(t3._2)
          if (t3._1.equals(EventEnum.pageView.toString))
            pv = pv + 1
          if (t3._1.equals(EventEnum.launch.toString)) nu = nu + 1
          sessionHashMap.put(t3._3, sessionHashMap.getOrElse(t3._3, 0) + 1)
        })
        resutSet(2) = nu
        resutSet(3) = pv
        (x._1, resutSet)
      })
    }, new HashPartitioner(sc.defaultParallelism), true).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val connection = JdbcManager.getConnection()
          val listBuffer = ListBuffer[StatsLocationFlow]()
          partition.foreach(t2 => {
            val uuidSet = t2._2(0).asInstanceOf[mutable.HashSet[String]]
            val sessionHashMap = t2._2(1).asInstanceOf[mutable.HashMap[String, Int]]
            val uv = uuidSet.size
            val nu = t2._2(2).asInstanceOf[Int]
            val pv = t2._2(3).asInstanceOf[Int]
            val sn = sessionHashMap.size
            var on = 0
            sessionHashMap.foreach(t2 => if (t2._2 == 1) on = on + 1)
            listBuffer.append(new StatsLocationFlow(
              DimensionConvert.getfindDimensionId(DateDimension.buildDate(TimeUtil.parseStringToLong(t2._1._1, "yyyy-MM-dd"), DateTypeEnum.day), connection),
              DimensionConvert.getfindDimensionId(new LocationDimension(t2._1._2, t2._1._3, t2._1._4), connection),
              uv,
              pv,
              nu,
              sn,
              on,
              new Date(TimeUtil.parseStringToLong(t2._1._1, "yyyy-MM-dd"))
            ))
          })
          //执行插入语句操作
          if (listBuffer.size > 0)
            StatsLocationFlowDao.setOrUpdateFlow(listBuffer)
          if (connection != null) connection.close()
        })
      }
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

}
