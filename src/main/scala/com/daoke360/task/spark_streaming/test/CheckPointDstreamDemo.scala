package com.daoke360.task.spark_streaming.test

import com.daoke360.utils.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ♠K on 2017-7-28.
  * QQ:272488352
  * 一， Spark-Streaming checkPoint概述
  * 每一个Spark Streaming应用，正常来说，都是要7 * 24小时运转的，这就是实时计算程序的特点。因为要持续不断的对数据进行计算。
  * 因此，对实时计算应用的要求，应该是必须要能够对与应用程序逻辑无关的失败，进行容错。
  * 如果要实现这个目标，Spark Streaming程序就必须将足够的信息checkpoint到容错的存储系统上，从而让它能够从失败中进行恢复。
  * 有两种数据需要被进行checkpoint：
  * 1、元数据checkpoint——将定义了流式计算逻辑的信息，保存到容错的存储系统上，比如HDFS。
  * 当运行Spark Streaming应用程序的Driver进程所在节点失败时，该信息可以用于进行恢复。元数据信息包括了：
  * .1 配置信息——创建Spark Streaming应用程序的配置信息，比如SparkConf中的信息。
  * .2 DStream的操作信息——定义了Spark Stream应用程序的计算逻辑的DStream操作信息。
  * .3 未处理的batch信息——那些job正在排队，还没处理的batch信息。
  *
  * 2，数据checkpoint——将实时计算过程中产生的RDD的数据保存到可靠的存储系统中。
  * 对于一些将多个batch的数据进行聚合的，有状态的transformation操作，这是非常有用的。
  * 在这种transformation操作中，生成的RDD是依赖于之前的batch的RDD的，这会导致随着时间的推移，RDD的依赖链条变得越来越长。
  * 要避免由于依赖链条越来越长，导致的一起变得越来越长的失败恢复时间，有状态的transformation操作执行过程中间产生的RDD，
  * 会定期地被checkpoint到可靠的存储系统上，比如HDFS。从而削减RDD的依赖链条，进而缩短失败恢复时，RDD的恢复时间。
  *
  * 一句话概括，元数据checkpoint主要是为了从driver失败中进行恢复；而RDD checkpoint主要是为了，使用到有状态的transformation操作时，
  * 能够在其生产出的数据丢失时，进行快速的失败恢复。
  *
  * 二，何时启用Checkpoint机制？
  * 1、使用了有状态的transformation操作——比如updateStateByKey，或者reduceByKeyAndWindow操作，被使用了，
  * 那么checkpoint目录要求是必须提供的，也就是必须开启checkpoint机制，从而进行周期性的RDD checkpoint。
  * 2、要保证可以从Driver失败中进行恢复——元数据checkpoint需要启用，来进行这种情况的恢复。
  * 要注意的是，并不是说，所有的Spark Streaming应用程序，都要启用checkpoint机制，如果即不强制要求从Driver失败中自动进行恢复，
  * 又没使用有状态的transformation操作，那么就不需要启用checkpoint。事实上，这么做反而是有助于提升性能的。
  *
  * 三，如何自动从Driver失败中恢复过来
  * 要能够自动从Driver失败中恢复过来，运行Spark Streaming应用程序的集群，就必须监控Driver运行的过程，并且在它失败时将它重启。
  * 对于Spark自身的standalone模式，需要进行一些配置去supervise driver，在它失败时将其重启。
  *
  * 首先，要在spark-submit中，添加--deploy-mode参数，默认其值为client，即在提交应用的机器上启动Driver；但是，要能够自动重启Driver，
  * 就必须将其值设置为cluster；此外，需要添加--supervise参数。
  *
  * 使用上述第二步骤提交应用之后，就可以让driver在失败时自动被重启，并且通过checkpoint目录的元数据恢复StreamingContext。
  *
  * spark-submit --master spark://mini1:7077 --total-executor-cores 15 --executor-memory 3 --deploy-mode cluster --supervise
  *
  *
  * 四，Checkpoint的说明
  *
  * 将RDD checkpoint到可靠的存储系统上，会耗费很多性能。当RDD被checkpoint时，会导致这些batch的处理时间增加。
  * 因此，checkpoint的间隔，需要谨慎的设置。对于那些间隔很多的batch，比如1秒，如果还要执行checkpoint操作，
  * 则会大幅度削减吞吐量。而另外一方面，如果checkpoint操作执行的太不频繁，那就会导致RDD的lineage变长，又会有失败恢复时间过长的风险。
  *
  * 对于那些要求checkpoint的有状态的transformation操作，默认的checkpoint间隔通常是batch间隔的数倍，至少是10秒。
  * 使用DStream的checkpoint()方法，可以设置这个DStream的checkpoint的间隔时长。通常来说，将checkpoint间隔设置为窗口操作的滑动间隔的5~10倍，
  * 是个不错的选择。
  *
  *
  *
  *
  *
  */
object CheckPointDstreamDemo {


  def createStreamingContext(sc: SparkContext, checkPointPath: String): StreamingContext = {

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(checkPointPath)

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "mini1:2181",
      "group.id" -> "gcjl",
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
    val wordDstream = ssc.union(dstrams).flatMap(_.split(" ")).map((_, 1))

    //Seconds(100) 对dstream进行checkpoint的时间间隔，时间不能设置太短，因为您需要往hdfs写数据，如果时间设置
    //太短会影响spark-streaming程序的性能和吞吐量
    wordDstream.checkpoint(Seconds(100))

    wordDstream.count()
    wordDstream.reduceByKey(_ + _).count()
    wordDstream.foreachRDD(rdd => {
      rdd.foreachPartition(par => par.foreach(println(_)))
    })
    ssc
  }

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HigthReliableDriverDemo").setMaster("local[8]")

    /**
      * 启用预写日志机制
      * 预写日志机制，简写为WAL，全称为Write Ahead Log。从Spark 1.2版本开始，就引入了基于容错的文件系统的WAL机制。如果启用该机制，
      * Receiver接收到的所有数据都会被写入配置的checkpoint目录中的预写日志。这种机制可以让driver在恢复的时候，避免数据丢失，
      * 并且可以确保整个实时计算过程中，零数据丢失。
      * *
      * 要配置该机制，首先要调用StreamingContext的checkpoint()方法设置一个checkpoint目录。然后需要将spark.streaming.receiver.writeAheadLog.enable
      * 参数设置为true。
      * *
      * 然而，这种极强的可靠性机制，会导致Receiver的吞吐量大幅度下降，因为单位时间内，有相当一部分时间需要将数据写入预写日志。
      * 如果又希望开启预写日志机制，确保数据零损失，又不希望影响系统的吞吐量，那么可以创建多个输入DStream，启动多个Rceiver。
      * *
      * 此外，在启用了预写日志机制之后，推荐将复制持久化机制禁用掉，因为所有数据已经保存在容错的文件系统中了，
      * 不需要在用复制机制进行持久化，保存一份副本了。只要将输入DStream的持久化机制设置一下即可，persist(StorageLevel.MEMORY_AND_DISK_SER)。
      */
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val sc = new SparkContext(sparkConf)

    val checkPoint = "hdfs://mini1:9000/checkPoint"
    //checkpoint 目录保存了我们的计算逻辑
    //使用这种StreamingContext.getOrCreate方式创建ssc,其实就是对我们的spark-steaming程序的driver提供容错功能
    //在里面你必须调用ssc.checkpoint()方法设置一个还原点目录,在这个还原点目录中，保存了spark-steaming程序的以下信息：
    //1 配置信息——创建Spark Streaming应用程序的配置信息，比如SparkConf中的信息。
    //2, DStream的操作信息——定义了Spark Stream应用程序的计算逻辑的DStream操作信息。
    //3,未处理的batch信息——那些job正在排队，还没处理的batch信息。
    val ssc = StreamingContext.getOrCreate(checkPoint, () => createStreamingContext(sc, checkPoint))
    ssc.start()
    ssc.awaitTermination()


  }
}
