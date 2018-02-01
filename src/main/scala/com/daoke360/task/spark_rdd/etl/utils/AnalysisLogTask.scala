package com.daoke360.task.spark_rdd.etl.utils

import java.util.zip.CRC32

import com.daoke360.common.{EventEnum, EventLogConstants, GlobalConstants}
import com.daoke360.conf.ConfigurationManager
import com.daoke360.task.spark_rdd.session.CategorySortKey
import com.daoke360.utils.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object AnalysisLogTask {
  val logger = Logger.getLogger(AnalysisLogTask.getClass)

  //保存数据的列簇
  private val family = Bytes.toBytes(EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME)
  //用于生成rowkey的CRC32 位数据校验
  private val crc32 = new CRC32

  //校验输入路径是否存在 /logs/2017/07/16
  def setInputPath(sparkConf: SparkConf): Unit = {

    val fileSystem = FileSystem.get(new Configuration())
    //获取运行时间
    val runDate = sparkConf.get(GlobalConstants.RUNNING_DATE_PARAMS)

    //2017-07-16---->2017/07/16
    val formatDate = TimeUtil.parseLongToString(TimeUtil.parseStringToLong(runDate), "yyyy/MM/dd")

    val path = new Path("/logs/" + formatDate)
    if (fileSystem.exists(path)) {
      sparkConf.set(GlobalConstants.HDFS_LOG_PATH, path.toString)
    } else {
      logger.error("目标路径不存在，请检查参数是否有误")
      return
    }
  }

  /**
    * 校验输入参数是否正确
    * -d 2017-07-14
    */
  def processArgs(args: Array[String], sparkConf: SparkConf) = {
    var runDate: String = null
    if (args.length >= 2 && args(0).equals("-d")) {
      runDate = args(1)
    }
    if (StringUtils.isBlank(runDate) || !TimeUtil.isValiDateRunningDate(runDate)) {
      runDate = TimeUtil.getYesterday
    }
    sparkConf.set(GlobalConstants.RUNNING_DATE_PARAMS, runDate)

  }

  /**
    * 将 信息解析成key value
    **/

  private def handleLogData(clientInfo: mutable.HashMap[String, String], eventName: String) = {
    //获取用户的uuid
    val uuid = clientInfo.getOrElse(EventLogConstants.LOG_COLUMN_NAME_UUID, null)
    //获取用户的memberId
    val mid = clientInfo.getOrElse(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID, null)
    //获取服务器时间
    val serverTime = clientInfo.getOrElse(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, null)
    //生成RowKey
    val rowKey = generateRowKey(uuid, mid, serverTime, eventName)
    val put = new Put(Bytes.toBytes(rowKey))
    for (kv <- clientInfo) {
      put.addColumn(family, Bytes.toBytes(kv._1), Bytes.toBytes(kv._2))
    }
    (new ImmutableBytesWritable(), put)
  }

  //生成rowkey的方法
  private def generateRowKey(uuid: String, mid: String, serverTime: String, eventName: String) = {
    val sb = new StringBuffer
    sb.append(serverTime + "_")
    this.crc32.reset()
    if (StringUtils.isNotBlank(uuid)) this.crc32.update(uuid.getBytes)
    if (StringUtils.isNotBlank(mid)) this.crc32.update(mid.getBytes)
    this.crc32.update(eventName.getBytes)
    sb.append(this.crc32.getValue % 10000000000000000L)
    sb.toString
  }

  def main(args: Array[String]): Unit = {


    // val spark = SparkSession.builder().appName("AnalysisLogTask").master("local").getOrCreate()
    /*val sc = spark.sparkContext*/

    val sparkConf = new SparkConf().setAppName("AnalysisLogTask").setMaster("local")
    //调节任务并行度
    sparkConf.set("spark.default.parallelism", "500")
    //指定序列化处理类
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(LoggerUtil.getClass, IPUtil.getClass, classOf[IPRule]))
    //校验输入参数是否正确
    this.processArgs(args, sparkConf)
    logger.info("正在处理" + sparkConf.get(GlobalConstants.RUNNING_DATE_PARAMS) + " 日的etl任务")
    //校验输入路径是否存在
    this.setInputPath(sparkConf)

    val conf = HBaseConfiguration.create()

    //设置连接hbase的zookeeper的地址
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_QUORUM, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_QUORUM))
    //设置连接hbase的zookeeper的端口
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY))

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, EventLogConstants.HBASE_EVENT_LOGS_TABLE)

    val sc = new SparkContext(sparkConf)

    val ipRules = sc.textFile("/spark_sf_project/1608c/resource/ip.data").map(line => {
      val fields: Array[String] = line.split("\\|")
      //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val startIP: Long = fields(2).toLong
      val endIP: Long = fields(3).toLong
      val country: String = fields(5)
      val province: String = fields(6)
      val city: String = fields(7)
      IPRule(startIP, endIP, country, province, city)
    }).collect()

    val ipRulesBroadCast = sc.broadcast[Array[IPRule]](ipRules)
    sc.textFile(sparkConf.get(GlobalConstants.HDFS_LOG_PATH)).map(line => {
      LoggerUtil.handleLog(line, ipRulesBroadCast.value)
    }).filter(x => x.size > 0).map(clientInfo => {
      val eventName = EventEnum.valueOfName(clientInfo(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME))
      handleLogData(clientInfo, eventName.toString) //(key,value)
    }).saveAsHadoopDataset(jobConf)


  }
}
