package com.daoke360.task.spark_sql

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.daoke360.Dao.{AreaTop3ProductDao, SparkTaskDao}
import com.daoke360.common.{EventLogConstants, GlobalConstants}
import com.daoke360.conf.ConfigurationManager
import com.daoke360.domain.AreaTop3Product
import com.daoke360.utils.{ParamUtil, TimeUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by ♠K on 2017-7-26.
  * QQ:272488352
  * 需求三，统计每个地区点击排名前三的商品
  * *
  *
  * 意义：
  * 这个需求还是很有必要，有时候特别是遇到双十一，或6.18这种电商店庆，那么就需要根据每个地区的销售商品
  * 提前备货，减轻物流压力，提高商品销售量和用户体验。
  * *
  * 实现思路
  * 1，过滤出pv事件
  * 2，将rdd数据封装dataframe
  * 3，定义UDAF函数对地区点击的城市进行聚合
  */
object AreaTop3ProductTask {
  private val logger = Logger.getLogger(AreaTop3ProductTask.getClass)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkconf = new SparkConf().setAppName("AreaTop3ProductTask").setMaster("local")
    processArgs(args, sparkconf)
    val taskId = sparkconf.get(GlobalConstants.RUN_TASK_ID)
    val sparkTask = SparkTaskDao.findTaskById(taskId.toLong)
    if (sparkTask == null) {
      logger.error("没有指定的日期")
      return
    }
    val jSONObject: JSONObject = JSON.parseObject(sparkTask.task_param)

    val scan = initScan(jSONObject)
    if (scan == null) {
      logger.error("这个扫描器有问题")
      return
    }
    val proScan = ProtobufUtil.toScan(scan)
    val strScan = Base64.encodeBytes(proScan.toByteArray)

    val conf = HBaseConfiguration.create()
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_QUORUM, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_QUORUM))
    conf.set(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY, ConfigurationManager.getProperty(GlobalConstants.HBASE_ZOOKEEPER_PROPERTY))
    conf.set(TableInputFormat.INPUT_TABLE, EventLogConstants.HBASE_EVENT_LOGS_TABLE)
    conf.set(TableInputFormat.SCAN, strScan)


    val sc = new SparkContext(sparkconf)
    val eventLogRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).map(_._2).map(result => {
      var eventName = Bytes.toString(result.getValue(EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME.getBytes(), EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
      var province = Bytes.toString(result.getValue(EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME.getBytes(), EventLogConstants.LOG_COLUMN_NAME_PROVINCE.getBytes()))
      var city = Bytes.toString(result.getValue(EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME.getBytes(), EventLogConstants.LOG_COLUMN_NAME_CITY.getBytes()))
      var goodsId = Bytes.toString(result.getValue(EventLogConstants.HBASE_EVENT_LOGS_FAMILY_NAME.getBytes(), EventLogConstants.LOG_COLUMN_NAME_GOODS.getBytes()))
      (eventName, province, city, goodsId)
    }).filter(tuple4 => tuple4._4 != null)

    val sqlContext = new SQLContext(sc)

    //生成商品点击基础信息表
    generatorGoodsClickTemp(eventLogRDD, sqlContext)

    //生成商品点击完整信息
    generatorGoodsClickFullInfoTemp(sqlContext)

    //生成商品点击完分组信息
    generatorGoodsClickGroupInfoTemp(sqlContext)

    //获取每个分组的前三名
    generatorTop3Temp(sqlContext, taskId.toLong)

  }

  //获取每个分组的前三名
  def generatorTop3Temp(sqlContext: SQLContext, taskId: Long): Unit = {
    AreaTop3ProductDao.deleteByTaskId(taskId)

    val url = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_URL, GlobalConstants.JDBC_BI))
    val user = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_USER, GlobalConstants.JDBC_BI))
    val password = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_PASSWORD, GlobalConstants.JDBC_BI))
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)

    sqlContext.sql(
      s"""
         |select $taskId as task_id,  province as area ,goodsId as product_id ,goods_name as product_name ,
         |cityInfo as city_infos, clickCount as click_count
         |from (
         |   select  province,goodsId,goods_name,cityInfo,clickCount,
         |   row_number()over(partition by province order by clickCount desc) rank
         |   from area_click_count_group_view
         | )t
         | where t.rank<=3
      """.stripMargin).write.mode(SaveMode.Append).jdbc(url, "area_top3_product", prop)

    /*      .foreachPartition(part => {

          val bufferList = new ArrayBuffer[AreaTop3Product]()
          part.foreach(row => {
            val province = row.getAs[String]("province")
            val productId = row.getAs[Long]("goodsId")
            val productName = row.getAs[String]("goods_name")
            val cityInfo = row.getAs[String]("cityInfo")
            val clickCount = row.getAs[Long]("clickCount")
            bufferList.append(AreaTop3Product(taskId, province, productId, cityInfo, clickCount, productName))
          })
          AreaTop3ProductDao.insert(bufferList.toArray)
        })*/

  }

  def generatorGoodsClickGroupInfoTemp(sqlContext: SQLContext) = {

    //注册用户自定义函数
    sqlContext.udf.register("concat_city_clickCount", (city: String, clickCount: Long) => {
      city + ":" + clickCount
    })
    //注册用户自定义聚合函数
    sqlContext.udf.register("group_concat", new GroupConcatCityUDAF)

    sqlContext.sql(
      """
        |select t.province,t.goodsId,t.goods_name,group_concat(cityInfo) cityInfo ,sum(clickCount) clickCount
        |from(
        |     select tmp.province,tmp.city,tmp.goodsId,tmp.goods_name,concat_city_clickCount(tmp.city,tmp.clickCount)cityInfo, tmp.clickCount
        |     from(
        |          select acfv.province,acfv.city,acfv.goodsId,acfv.goods_name,count(*) clickCount
        |          from area_click_full_info_view acfv
        |          group by province,city,goodsId,goods_name
        |      ) tmp
        |)t group by province,goodsId,goods_name
      """.stripMargin).createOrReplaceTempView("area_click_count_group_view")

  }

  /**
    * 生成商品点击完整信息
    */
  def generatorGoodsClickFullInfoTemp(sqlContext: SQLContext) = {
    val url = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_URL, GlobalConstants.JDBC_ECS))
    val user = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_USER, GlobalConstants.JDBC_ECS))
    val password = ConfigurationManager.getProperty(String.format(GlobalConstants.JDBC_PASSWORD, GlobalConstants.JDBC_ECS))

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)

    sqlContext.read.jdbc(url, "ecs_goods", prop).createOrReplaceTempView("ecs_goods_view")

    sqlContext.sql(
      """
        |select acv.province,acv.city,acv.goodsId,eg.goods_name
        |from area_click_view acv
        |join ecs_goods_view eg on acv.goodsId=eg.goods_id
      """.stripMargin).createOrReplaceTempView("area_click_full_info_view")


  }


  /**
    * 生成商品点击基础信息表
    */
  def generatorGoodsClickTemp(eventLogRDD: RDD[(String, String, String, String)], sqlContext: SQLContext) = {

    val goodsClickRowRDD = eventLogRDD.map(tuple4 => Row(tuple4._2, tuple4._3, tuple4._4.toLong))

    //定义schema
    val schema = StructType(List(
      StructField("province", StringType, true),
      StructField("city", StringType, true),
      StructField("goodsId", LongType, true)
    ))
    //将rdd和schema进行关联创建dataFrame
    val df = sqlContext.createDataFrame(goodsClickRowRDD, schema)
    //注册临时表
    df.createOrReplaceTempView("area_click_view")
  }


  def initScan(jSONObject: JSONObject) = {
    var scan: Scan = null
    val startDate: String = ParamUtil.getParam(GlobalConstants.START_DATE, jSONObject).toString
    val endDate = ParamUtil.getParam(GlobalConstants.END_DATE, jSONObject).toString
    if (startDate != null && endDate != null) {
      scan = new Scan()
      val startTime = TimeUtil.parseStringToLong(startDate, "yyyy-MM-dd HH:mm:ss") + ""
      val endTime = TimeUtil.parseStringToLong(endDate, "yyyy-MM-dd HH:mm:ss") + ""
      scan.setStartRow(startTime.getBytes())
      scan.setStopRow(endTime.getBytes())
      val eventArr = Array(
        EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
        EventLogConstants.LOG_COLUMN_NAME_GOODS,
        EventLogConstants.LOG_COLUMN_NAME_PROVINCE,
        EventLogConstants.LOG_COLUMN_NAME_CITY
      )
      val array = new Array[Array[Byte]](eventArr.length)
      for (i <- 0 until (array.length)) {
        array(i) = eventArr(i).getBytes()
      }
      val multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(array)
      scan.setFilter(multipleColumnPrefixFilter)
    }
    scan
  }

  def processArgs(args: Array[String], sparkconf: SparkConf): Unit = {

    if (args.length >= 2 && args(0).equals(GlobalConstants.RUN_PARAMS_FLAG) && ParamUtil.isValidateNumber(args(1))) {
      sparkconf.set(GlobalConstants.RUN_TASK_ID, args(1))
    } else {
      logger.error("please check your params")
      return
    }


  }

}
