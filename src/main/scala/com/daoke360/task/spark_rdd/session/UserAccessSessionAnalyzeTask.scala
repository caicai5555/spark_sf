package com.daoke360.task.spark_rdd.session

import java.sql.{Connection, DriverManager, ResultSet}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.daoke360.Dao.{SessionAggrStatDao, SparkTaskDao, Top5CategoryDao}
import com.daoke360.common.{EventEnum, EventLogConstants, GlobalConstants}
import com.daoke360.conf.ConfigurationManager
import com.daoke360.domain.{SessionAggrStat, Top5Category}
import com.daoke360.utils.{NumberUtil, ParamUtil, StringUtil, TimeUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, MultipleColumnPrefixFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{Accumulable, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * Created by ♠K on 2017-7-17.
  * QQ:272488352
  *
  * 统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m
  * 以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比
  */
object UserAccessSessionAnalyzeTask {
  val logger = Logger.getLogger(UserAccessSessionAnalyzeTask.getClass)
  //表的列簇 log
  val family = EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME.getBytes


  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.WARN)
    var sparkConf = new SparkConf().setAppName("UserAccessSessionAnalyzeTask").setMaster("local")
    //调节任务并行度
    sparkConf.set("spark.default.parallelism", "500")
    //指定序列化处理类
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CategorySortKey], classOf[CategorySortKey]))
    //开启推测机制
    sparkConf.set("spark.speculation", "true")
    //校验输入参数
    processArgs(args, sparkConf)
    //从javaEE系统中把任务参数获取出来
    val sparkTask = SparkTaskDao.findTaskById(sparkConf.get(GlobalConstants.RUN_TASK_ID).toLong)
    if (sparkTask == null) {
      logger.error("No corresponding task was found..")
      return
    }
    //{"startDate":"2017-07-16 00:00:00","endDate":"2017-07-16 23:59:59"}
    val parseObject: JSONObject = JSON.parseObject(sparkTask.task_param)
    val scan = initScan(parseObject)
    if (scan == null) return

    val proScan = ProtobufUtil.toScan(scan)
    val serScan = Base64.encodeBytes(proScan.toByteArray)

    val conf = HBaseConfiguration.create()
    //zk的连接地址
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_QUORUM, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_QUORUM))
    //zk的连接端口
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY))
    //设置扫描器
    conf.set(TableInputFormat.SCAN, serScan)
    conf.set(TableInputFormat.INPUT_TABLE, EventLogConstants.HBASE_EVENT_LOGS_TABLE)

    val sc = new SparkContext(sparkConf)

    val eventLogRDD = sc.newAPIHadoopRDD(
      conf, //配置对象
      classOf[TableInputFormat], //使用哪个类去hbasle读取数据
      classOf[ImmutableBytesWritable], //输入的key的类型
      classOf[Result] //这个是输入的value的类型
    ).map(_._2).map(result => {
      val eventName = Bytes.toString(result.getValue(family, EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
      val uuid = Bytes.toString(result.getValue(family, EventLogConstants.LOG_COLUMN_NAME_UUID.getBytes))
      val sessionId = Bytes.toString(result.getValue(family, EventLogConstants.LOG_COLUMN_NAME_SID.getBytes()))
      val serverTime = Bytes.toString(result.getValue(family, EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME.getBytes()))
      val goodsId = Bytes.toString(result.getValue(family, EventLogConstants.LOG_COLUMN_NAME_GOODS.getBytes))
      (sessionId, (uuid, sessionId, serverTime, goodsId, eventName))
    })


    /**
      * 由于sessionidActionRDD会被多次引用，那么意味着它可能多次从hbase中读取数据
      * 那么这个读取过程是非常耗时的，在这里可以对读取出来的数据进行持久化，以便下次可以直接从内存或磁盘中读取
      */
    eventLogRDD.cache()

    //将session的多次访问记录压缩成一条记录(sessionId,uuid=akdfaf|sid=asdfa|...)
    val sessionFullAggrInfoRDD = aggregateBySession(eventLogRDD)

    //定义一个累加器
    val sessionAggrStatAccumulator = sc.accumulable("")(SessionAggrStatAccumulator)

    sessionFullAggrInfoRDD.foreach(tuple2 => {
      val sessionFullAggrInfo = tuple2._2
      sessionAggrStatAccumulator.add(GlobalConstants.SESSION_COUNT)
      //获取步长和时长
      val stepLength = StringUtil.getAccumulatorFieldValue(sessionFullAggrInfo, "\\|", GlobalConstants.FIELD_PARAM_STEP_LENGTH)
      //计算步长所属范围区间，并添加到累加器中
      calculateStepLength(stepLength.toInt, sessionAggrStatAccumulator)

      val visitLength = StringUtil.getAccumulatorFieldValue(sessionFullAggrInfo, "\\|", GlobalConstants.FIELD_PARAM_VISIT_LENGTH)
      calculateVisitLength(visitLength.toInt, sessionAggrStatAccumulator)
    })

    //计算访问时长和步长占比，并将结果保存到mysql中去
    calculateAndPersistAggrStat(sparkTask.task_id, sessionAggrStatAccumulator.value)


    //计算点击，购物车，支付排名前五的品类
    getTop5Category(sparkTask.task_id, eventLogRDD, sc)


  }

  /**
    * 计算每个品类点击，加入购物车，支付的总次数
    */
  def joinCategoryAndData(categoryDistinctRDD: RDD[(Long, Long)],
                          categoryClickRDD: RDD[(Long, Int)], categoryCartRDD: RDD[(Long, Int)]) = {

    categoryDistinctRDD.leftOuterJoin(categoryClickRDD).map(tuple2 => {
      //tuple2===>(categoryId,(categoryId,Some(100)/None))
      val categoryId = tuple2._1
      val count = tuple2._2._2
      var clickCount = 0
      if (!count.isEmpty) {
        clickCount = count.get
      }
      val value = GlobalConstants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + GlobalConstants.FIELD_CLICK_COUNT + "=" + clickCount
      (categoryId, value)
    }).leftOuterJoin(categoryCartRDD).map(tuple2 => {
      //tuple2==>(categoryId,(categoryid=1|clickCount=100,Some(100)/None))
      val categoryId = tuple2._1
      val count = tuple2._2._2
      var cartCount = 0
      if (!count.isEmpty) {
        cartCount = count.get
      }
      val value = tuple2._2._1 + "|" + GlobalConstants.FIELD_CART_COUNT + "=" + cartCount
      (categoryId, value)
    })
  }

  /**
    * 计算点击，购物车，支付排名前五的品类
    *
    * @param task_id
    * @param eventLogRDD (sessionId, (uuid, sessionId, serverTime, goodsId, eventName))
    * @param sc
    * @return
    */
  def getTop5Category(task_id: Long, eventLogRDD: RDD[(String, (String, String, String, String, String))], sc: SparkContext) = {
    val connection = () => {
      //注册jdbc驱动
      Class.forName(ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_DRIVER, GlobalConstants.JDBC_ECS)))
      val url = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_URL, GlobalConstants.JDBC_ECS))
      val user = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_USER, GlobalConstants.JDBC_ECS))
      val password = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_PASSWORD, GlobalConstants.JDBC_ECS))
      DriverManager.getConnection(url, user, password)
    }

    //从msyql的ecs数据中读取goods的数据
    val goodsInfoRDD = new JdbcRDD[(Long, Long)](
      sc, connection,
      "select goods_id,cat_id from ecs_goods where goods_id>= ? and goods_id<=? ", 0, 1000000, 2,
      (resultSet: ResultSet) => {
        val goods_id = resultSet.getLong("goods_id")
        val cat_id = resultSet.getLong("cat_id")
        (goods_id, cat_id)
      }
    )

    // eventLogRDD  (sessionId, (uuid, sessionId, serverTime, goodsId, eventName))
    val goodsLogRDD = eventLogRDD.filter(tuple2 => tuple2._2._4 != null).map(tuple2 => (tuple2._2._4.toLong, tuple2._2._5))
    //(goodsId,(eventName,cat_id))
    val categoryAllRDD = goodsLogRDD.join(goodsInfoRDD)

    //这里面包含了 被点击，加入购物车，支付的品类
    val categoryDistinctRDD = categoryAllRDD.map(tuple2 => (tuple2._2._2, tuple2._2._2)).distinct()

    //计算每个品类被点击(浏览)的次数
    val categoryClickRDD = categoryAllRDD.filter(tuple2 => tuple2._2._1.equals(EventEnum.pageView.toString)).map(tuple2 => (tuple2._2._2, 1)).reduceByKey(_ + _)

    //计算每个品类加入购物车的次数
    val categoryCartRDD = categoryAllRDD.filter(tuple2 => tuple2._2._1.equals(EventEnum.addToCart.toString)).map(tuple2 => (tuple2._2._2, 1)).reduceByKey(_ + _)

    //计算每个品类被支付的次数
    //categoryAllRDD.filter(tuple2 => tuple2._2._1.equals(EventEnum.toString)).map(tuple2 => (tuple2._2._2, 1)).reduceByKey(_ + _)

    //计算每个品类点击，加入购物车，支付的总次数(categoryId,categoryId=1|clickCount=1|cartCount=1)
    val categoryAllInfoRDD: RDD[(Long, String)] = joinCategoryAndData(categoryDistinctRDD, categoryClickRDD, categoryCartRDD)
    val top5CategoryListBuffer = new ListBuffer[Top5Category]()

    categoryAllInfoRDD.map(tuple2 => {
      val sb = tuple2._2
      val clickCount = StringUtil.getAccumulatorFieldValue(sb, "\\|", GlobalConstants.FIELD_CLICK_COUNT).toLong
      val cartCount = StringUtil.getAccumulatorFieldValue(sb, "\\|", GlobalConstants.FIELD_CART_COUNT).toLong
      (new CategorySortKey(clickCount, cartCount, 0), tuple2._1)
    }).sortByKey(false).take(5).foreach(tuple2 => {
      val top5Category = new Top5Category(
        task_id,
        tuple2._2,
        tuple2._1.clickCount,
        tuple2._1.cartCount,
        tuple2._1.payCount
      )
      top5CategoryListBuffer.append(top5Category)
    })
    //删除对应的任务记录
    Top5CategoryDao.deleteByTaskId(task_id)
    //将top5插入到mysql中
    Top5CategoryDao.insert(top5CategoryListBuffer)
  }

  /**
    * 计算访问时长和步长占比，并将结果保存到mysql中去
    *
    * @param task_id
    * @param value session_count=15|1s_3s=4|4s_6s=2|7s_9s=0|10s_30s=0|30s_60s=2|1m_3m=5|3m_10m=2|10m_30m=0|30m=0|1_3=8|4_6=1|7_9=1|10_30=5|30_60=0|60=0
    */
  def calculateAndPersistAggrStat(task_id: Long, value: String) = {
    // 从Accumulator统计串中获取值
    val session_count: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.SESSION_COUNT).toLong

    val visit_length_1s_3s: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_1s_3s).toLong
    val visit_length_4s_6s: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_4s_6s).toLong
    val visit_length_7s_9s: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_7s_9s).toLong
    val visit_length_10s_30s: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_10s_30s).toLong
    val visit_length_30s_60s: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_30s_60s).toLong
    val visit_length_1m_3m: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_1m_3m).toLong
    val visit_length_3m_10m: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_3m_10m).toLong
    val visit_length_10m_30m: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_10m_30m).toLong
    val visit_length_30m: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.TIME_PERIOD_30m).toLong

    val step_length_1_3: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_1_3).toLong
    val step_length_4_6: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_4_6).toLong
    val step_length_7_9: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_7_9).toLong
    val step_length_10_30: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_10_30).toLong
    val step_length_30_60: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_30_60).toLong
    val step_length_60: Long = StringUtil.getAccumulatorFieldValue(value, "\\|", GlobalConstants.STEP_PERIOD_60).toLong

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio: Double = NumberUtil.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtil.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtil.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtil.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtil.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtil.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtil.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtil.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio: Double = NumberUtil.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)

    val step_length_1_3_ratio: Double = NumberUtil.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio: Double = NumberUtil.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio: Double = NumberUtil.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio: Double = NumberUtil.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio: Double = NumberUtil.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio: Double = NumberUtil.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.taskid = task_id
    sessionAggrStat.session_count = session_count
    sessionAggrStat.visit_length_1s_3s_ratio = visit_length_1s_3s_ratio
    sessionAggrStat.visit_length_4s_6s_ratio = visit_length_4s_6s_ratio
    sessionAggrStat.visit_length_7s_9s_ratio = visit_length_7s_9s_ratio
    sessionAggrStat.visit_length_10s_30s_ratio = visit_length_10s_30s_ratio
    sessionAggrStat.visit_length_30s_60s_ratio = visit_length_30s_60s_ratio
    sessionAggrStat.visit_length_1m_3m_ratio = visit_length_1m_3m_ratio
    sessionAggrStat.visit_length_3m_10m_ratio = visit_length_3m_10m_ratio
    sessionAggrStat.visit_length_10m_30m_ratio = visit_length_10m_30m_ratio
    sessionAggrStat.visit_length_30m_ratio = visit_length_30m_ratio
    sessionAggrStat.step_length_1_3_ratio = step_length_1_3_ratio
    sessionAggrStat.step_length_4_6_ratio = step_length_4_6_ratio
    sessionAggrStat.step_length_7_9_ratio = step_length_7_9_ratio
    sessionAggrStat.step_length_10_30_ratio = step_length_10_30_ratio
    sessionAggrStat.step_length_30_60_ratio = step_length_30_60_ratio
    sessionAggrStat.step_length_60_ratio = step_length_60_ratio
    SessionAggrStatDao.deleteByTaskId(task_id)
    SessionAggrStatDao.insert(sessionAggrStat)
  }

  /**
    * 计算步长所属的范围区间
    */
  def calculateVisitLength(visitLength: Int, sessionAggrStatAccumulator: Accumulable[String, String]) = {
    if (visitLength >= 0 && visitLength <= 3) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 1 * 60 && visitLength <= 3 * 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 3 * 60 && visitLength <= 10 * 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 10 * 60 && visitLength <= 30 * 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 30 * 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.TIME_PERIOD_30m)
    }

  }

  /**
    * 计算步长所属的范围区间
    */
  def calculateStepLength(stepLength: Int, sessionAggrStatAccumulator: Accumulable[String, String]) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAggrStatAccumulator.add(GlobalConstants.STEP_PERIOD_60)
    }
  }


  /**
    * 将session的多次访问记录压缩成一条记录
    *
    * @param eventLogRDD
    */
  def aggregateBySession(eventLogRDD: RDD[(String, (String, String, String, String, String))]) = {
    //按key进行分组 (sessionId,( (uuid, sessionId, serverTime, goodsId), (uuid, sessionId, serverTime, goodsId)。。。))

    val sessionRDD = eventLogRDD.groupByKey()
    sessionRDD.map(tuple2 => {
      val sessionId = tuple2._1
      //用于封装用户本次session的点击商品ID
      var clickGoodsIdsBuffer = new StringBuffer()
      //用于封装用户本次搜索过的关键词
      var searchKeywordBuffer = new ListBuffer[String]()
      //用于封装用户本次点击的品类ID
      var clickCategoryIdsBuffer = new ListBuffer[String]()

      //定义本次session的开始时间和结束时间
      var startTime, endTime: Long = 0L
      //定义访问时长
      var visitLength: Long = 0L
      //定义访问步长
      var stepLength: Long = 0L

      var uuid: String = null
      tuple2._2.foreach(tuple5 => {
        //(uuid, sessionId, serverTime, goodsId)
        uuid = tuple5._1
        val serverTime = tuple5._3.toLong
        if (StringUtils.isNotBlank(tuple5._4)) {
          clickGoodsIdsBuffer.append(tuple5._4 + ",")
        }
        if (startTime == 0) startTime = serverTime
        if (endTime == 0) endTime = serverTime
        if (serverTime < startTime) startTime = serverTime
        if (serverTime > endTime) endTime = serverTime
        if (tuple5._5.equals(EventEnum.pageView.toString)) {
          stepLength += 1
        }
      })
      //计算访问时长
      visitLength = (endTime - startTime) / 1000
      //(sid,sid=adada|uuid=akdfa|clickGoodsIds=1,2,3,4|stepLength=5|visitLength=6|..)
      val sessionFullInfo = new StringBuffer()
      //1,2,3,
      var clickGoodsIds = clickGoodsIdsBuffer.toString()
      if (clickGoodsIds.length > 0) {
        clickGoodsIds = clickGoodsIds.substring(0, clickGoodsIds.length - 1)
      }
      sessionFullInfo.append(GlobalConstants.FIELD_PARAM_SID + "=" + sessionId + "|")
      sessionFullInfo.append(GlobalConstants.FIELD_PARAM_UUID + "=" + uuid + "|")
      sessionFullInfo.append(GlobalConstants.FIELD_PARAM_CLICK_GOODS_IDS + "=" + clickGoodsIds + "|")
      sessionFullInfo.append(GlobalConstants.FIELD_PARAM_STEP_LENGTH + "=" + stepLength + "|")
      sessionFullInfo.append(GlobalConstants.FIELD_PARAM_VISIT_LENGTH + "=" + visitLength + "|")
      (sessionId, sessionFullInfo.toString)
    })


  }

  /**
    * 设置扫描器
    */
  def initScan(param: JSONObject): Scan = {
    var scan: Scan = null
    val startDate = ParamUtil.getParam(GlobalConstants.START_DATE, param)
    val endDate = ParamUtil.getParam(GlobalConstants.END_DATE, param)
    if (startDate == null || endDate == null) {
      logger.error("The task parameter is incorrect. Check that the parameters are correct")
    } else {
      scan = new Scan()
      val longStartDateTime = TimeUtil.parseStringToLong(startDate.toString, "yyyy-MM-dd HH:mm:ss") + ""
      val longEndDateTime = TimeUtil.parseStringToLong(endDate.toString, "yyyy-MM-dd HH:mm:ss") + ""
      scan.setStartRow(longStartDateTime.getBytes())
      scan.setStopRow(longEndDateTime.getBytes())
      //设置扫描的表
      val columns = Array(
        EventLogConstants.LOG_COLUMN_NAME_UUID,
        EventLogConstants.LOG_COLUMN_NAME_SID,
        EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
        EventLogConstants.LOG_COLUMN_NAME_GOODS,
        EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME
      )
      val array = new Array[Array[Byte]](columns.length)
      for (i <- 0 until (columns.length)) {
        array(i) = columns(i).getBytes()
      }
      val multipleColumnPrefixFilter: MultipleColumnPrefixFilter = new MultipleColumnPrefixFilter(array)
      scan.setFilter(multipleColumnPrefixFilter)
    }
    scan
  }

  def processArgs(args: Array[String], sparkConf: SparkConf): Unit = {

    if (args.length >= 2 && args(0).equals(GlobalConstants.RUN_PARAMS_FLAG) && ParamUtil.isValidateNumber(args(1))) {
      sparkConf.set(GlobalConstants.RUN_TASK_ID, args(1))
    } else {
      logger.error("The parameter is abnormal. Check that the parameters are correct. Quit the task")
      return
    }
  }

}
