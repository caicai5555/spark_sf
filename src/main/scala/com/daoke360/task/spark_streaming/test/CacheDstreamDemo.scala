package com.daoke360.task.spark_streaming.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ♠K on 2017-7-28.
  * QQ:272488352
  *
  * 和rdd类似，如果某个dsteam计算特别复杂，依赖链条特别长，会被多次引用
  * 此时我们对这个dsteam做持久化
  *
  */
object CacheDstreamDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(2))
    //spark-streaming 在创建ds输入流的时候，receiver会将接收到的数据默认会保存一个副本，即相当于StorageLevel.MEMORY_ONLY_SER_2
    val ds = ssc.socketTextStream("mini1", 8080)


    val wordDstream = ds.flatMap(_.split(" ")).map((_, 1))
    wordDstream.cache()

    wordDstream.count()
    wordDstream.reduceByKey(_ + _).count()
    wordDstream.foreachRDD(rdd => {
      rdd.foreachPartition(par => par.foreach(println(_)))
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
