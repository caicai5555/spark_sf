package com.daoke360.task.spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{RuntimeConfig, SQLContext, SparkSession}

/**
  * Created by ♠K on 2017-7-20.
  * QQ:272488352
  */
object Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("").master("local").config("","true").getOrCreate()
    val sparkConf: RuntimeConfig = spark.conf
    val sc = spark.sparkContext
    val sqlContext: SQLContext = spark.sqlContext
    sqlContext.sql("").createOrReplaceTempView("") //registerTempTable("")


    /*    val sparkConf = new SparkConf().setAppName("").setMaster("local")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("group_concat_disinct", new GroupConcatDistinctUDAF())
        val arr = Array(Row(1, "北京"), Row(1, "上海"), Row(2, "西安"), Row(2, "杭州"))
        val rdd1 = sc.parallelize(arr)
        val schema = StructType(List(StructField("id", IntegerType, true), StructField("city", StringType, true)))
        sqlContext.createDataFrame(rdd1, schema).registerTempTable("citys")
        sqlContext.sql("select id,group_concat_disinct(city) from citys group by id").show()*/
    sc.stop()
  }

}
