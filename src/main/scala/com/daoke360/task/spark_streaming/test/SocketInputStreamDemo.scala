package com.daoke360.task.spark_streaming.test

import com.daoke360.utils.LoggerLevels
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ♠K on 2017-7-27.
  * QQ:272488352
  * spark-streaming的上下文：SteamingContext ，底层依赖SparkContext
  * 一个程序中只能有一个SteamingContext对象
  *
  * Dstream：离散的数据流，代表一个源源不断的数据输入源。每隔batchInter 间隔就会从Dstream中产生一个rdd
  *
  *
  *
  *
  */
object SocketInputStreamDemo {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //会创建两个特别重要的对象，JobScheduler（用来做任务的提交）和JobGenerator（生成任务）
    val ssc = new StreamingContext(sc, Seconds(2))
    //以下这些代码只是定义了dstream的计算逻辑，并不会真正的执行
    ssc.socketTextStream("mini1", 8989)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        //连接对象不能放在这，因为连接对象是不能被序列化的
        rdd.foreachPartition(part => {
          //一般连接对象放在这里，即每个分区上创建一个连接对象
          part.foreach(tuple2 => {
            //不要把连接对象放在这里面，放在这里意味着每条数据都要创建一个连接对象，效率严重不高
            println(tuple2)
          })
        })
      })

    /**
      * 在调用streamingContext对象的时候，会创建一个特别重要的对象DstreamGraph，并将dstream的转换关系保存
      * 到DstreamGraph以及这个dstream所绑定的receiver也会被添加到DstreamGraph
      *
      * 创建成功了DstreamGraph对象之后会立马创建一个特别特别重要的重量级对象ReceiverTracker
      * ReceiverTracker 会从DstreamGraph中把对应的receiver做成job，通过spark-core那一套提交任务的方法
      * 将receiver作为任务发送到指定的executor上启动
      */
    ssc.start()

    //等待停止输入名 Kill
    ssc.awaitTermination()
  }
}

