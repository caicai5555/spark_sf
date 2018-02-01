package com.daoke360.task.spark_streaming.test

import com.daoke360.utils.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ♠K on 2017-7-27.
  * QQ:272488352
  *
  */
object KafkaInputStreamDemo {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("").setMaster("local[9]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "mini1:2181",
      "group.id" -> "w1608",
      "auto.offset.reset" -> "largest"
    )

    val topicPartition = 3

    //启动三个receiver对topic进行消费
    val dstrams = (0 to 2).map(num => {
      val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
        kafkaParams, Map("wordcount" -> 1), StorageLevel.MEMORY_AND_DISK).map(_._2)
      dstream
    })

    //对合并完的消息进行处理
    ssc.union(dstrams).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()


    //这种方式是使用kafka的高阶Api创建输入的dstream
    /*    val kafkaDstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
          kafkaParams, Map("wordcount" -> 3), StorageLevel.MEMORY_AND_DISK)
        //kafk的消息是key和value对，value才是真正保存message的，所有取第二个即可
        kafkaDstream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreachRDD(
          rdd => {
            rdd.foreachPartition(
              part => {
                part.foreach(println(_))
              }
            )
          }
        )*/

    ssc.start()
    ssc.awaitTermination()

  }
}
