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
object HigthReliableDriverDemo {

  def createStreamingContext(sc: SparkContext, checkPointPath: String): StreamingContext = {

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(checkPointPath)

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
    ssc
  }

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HigthReliableDriverDemo").setMaster("local[8]")
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val sc = new SparkContext(sparkConf)

    val checkPoint = "hdfs://mini1:9000/checkPoint"
    //checkpoint 目录保存了我们的计算逻辑
    val ssc = StreamingContext.getOrCreate(checkPoint, () => createStreamingContext(sc, checkPoint))

    ssc.start()
    ssc.awaitTermination()

  }
}
