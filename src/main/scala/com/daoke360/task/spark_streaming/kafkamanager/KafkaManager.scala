package com.daoke360.task.spark_streaming.kafkamanager

import java.util.concurrent.atomic.AtomicReference

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by ♠K on 2017-7-31.
  * QQ:272488352
  * 1，创建输入的dstream
  * 2，更新过时的消费者偏移量
  * 3，获取每个分区的消费偏移量
  * 4，定时更新每个分区的消费偏移量到zk上
  */
class KafkaManager(var kafkaParams: Map[String, String], var topicSet: Set[String]) extends Serializable {
  val groupId = if (kafkaParams.contains("group.id")) kafkaParams("group.id") else throw new SparkException("No consumer groups.id")

  //创建kafka集群客户端
  val kafkaCluster = new KafkaCluster(kafkaParams)

  //streaming程序启动后应该立马去set或更新消费偏移量的起始位置
  setOrUpdateOffsetToZK()

  //打印消费的起始位置
  printZKOffset(getConsumerOffsets())

  /**
    * streaming程序启动后应该立马去set或更新消费偏移量的起始位置
    */
  def setOrUpdateOffsetToZK() = {
      //曾经是否消费过
      var hasCousumed = true
      //获取每个topic的分区数
      val errOrPartitions = kafkaCluster.getPartitions(topicSet)
      //判断否是获取到了topic的分区
      if (errOrPartitions.isLeft) throw new SparkException("The specified partition was not found")
      //获取每个分区
      val partitions: Set[TopicAndPartition] = errOrPartitions.right.get
      //尝试获取每个分区的消费偏移量
      val errOrAndPartitionToLong: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupId, partitions)
      if (errOrAndPartitionToLong.isLeft) hasCousumed = false

      //用来存放需要被重置的分区消费偏移量
      val offsets = new mutable.HashMap[TopicAndPartition, Long]()

      if (hasCousumed) {
        //消费过
        //获取每个分区的消费偏移量Map(part-0->10, part-1->20,  part-2->30   )
        val consumerPartitionToLong: Map[TopicAndPartition, Long] = errOrAndPartitionToLong.right.get

        //尝试获取每个topic每个分区的最早的一条消息的偏移量
        val errOrAndPartitionToOffset = kafkaCluster.getEarliestLeaderOffsets(partitions)
        //获取每个topic的最早的一条消息的偏移量失败
        if (errOrAndPartitionToOffset.isLeft) throw new SparkException("Failed to get the partition offset")
        //获取的是每个分区leader的最早一条消息的偏移量 Map( part-0->12, part-1->0,  part-2->0   )
        val earliestLeaderPartitionToOffset: Map[TopicAndPartition, LeaderOffset] = errOrAndPartitionToOffset.right.get
        //使用每个分区的消费偏移量和每个分区的最早偏移量进行对比，如果消费偏移量比对应的分区最早偏移量小，意味着该分区有部分数据以及加入到了kafka
        //定时清理策略中，即消费偏移量过时，此时应该将消费偏移量设置成分区最早的那一条消息的offset
        consumerPartitionToLong.foreach(t2 => {
          //t2 即某个分区的消费偏移量（TopicAndPartition, Long）
          //获取该分区最早的一条消息偏移量
          val leaderOffset = earliestLeaderPartitionToOffset(t2._1).offset
          //该分区的某些消息过时，被加入到kafka的清理策略中
          if (t2._2 < leaderOffset) {
            offsets.put(t2._1, leaderOffset)
          }
        })
      } else {

        var partitionToLeaderOffset: Map[TopicAndPartition, LeaderOffset] = null
        //没有消费过，用户手动指定消费偏移量或者从最大的位置开始消费
        val reset = kafkaParams.get("group.id")
        if (reset == Some("smallest")) {
          //尝试获取每个分区最早的offset
          val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(partitions)
          if (earliestLeaderOffsets.isLeft) throw new SparkException("Failed to get the partition earliestLeaderOffsets")
          partitionToLeaderOffset = earliestLeaderOffsets.right.get
        } else {
          //用户没有指定消费位置，默认从最后开始消费
          //尝试获取每个分区的最大消息偏移量
          val latestLeaderOffsets = kafkaCluster.getLatestLeaderOffsets(partitions)
          if (latestLeaderOffsets.isLeft) throw new SparkException("Failed to get the partition latestLeaderOffsets")
          partitionToLeaderOffset = latestLeaderOffsets.right.get
        }
        partitionToLeaderOffset.map(x => (x._1, x._2.offset)).foreach(x => offsets.put(x._1, x._2))
      }
      //重置过时的消费偏移量到zk上
      kafkaCluster.setConsumerOffsets(groupId, offsets.toMap)

  }

  /**
    * 打印每个分区的开始消费位置
    */
  def printZKOffset(consumerPartitionOffsets: Map[TopicAndPartition, Long]) = {
    println("====================================================")
    consumerPartitionOffsets.foreach(t2 => {
      println(s"topic:${t2._1.topic},partition:${t2._1.partition},beginOffset:${t2._2}")
    })
    println("====================================================")
  }

  /**
    * 获取消费起始位置
    */
  def getConsumerOffsets(): Map[TopicAndPartition, Long] = {
    val consumerPartitionOffsets = new mutable.HashMap[TopicAndPartition, Long]()
    //尝试获取topic的每个分区
    val partitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicSet)
    //获取分区失败
    if (partitions.isLeft) throw new SparkException("The specified partition was not found")
    //获取分区
    val topicAndPartitions = partitions.right.get
    //尝试获取每个分区的消费位置
    val errOrPartitionToLong = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitions)
    if (errOrPartitionToLong.isLeft) {
      topicAndPartitions.foreach(tp => {
        consumerPartitionOffsets.put(tp, 0L)
      })
    } else {
      errOrPartitionToLong.right.get.foreach(t2 => consumerPartitionOffsets.put(t2._1, t2._2))
    }
    consumerPartitionOffsets.toMap
  }


  val offsets = new AtomicReference[Array[OffsetRange]]()

  /**
    * 创建输入的Dsteaming
    *
    * <: 上边界
    *
    * <% 下边界
    */
  def createDirectStream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext) = {
    KafkaUtils.createDirectStream[K, V, KD, VD, V](ssc, kafkaParams, getConsumerOffsets, (messageHandler: MessageAndMetadata[K, V]) => {
      messageHandler.message()
    })
      .transform(rdd => {
        //记录当前rdd消费的偏移量，以便及时更新到zk上
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsets.set(offsetRanges)
        rdd
      })
  }

  /**
    * 更新offset到zk上
    */
  def updateOffsetToZK() = {
    println("================开始更新offset到zk上================================")
    val consumerPartitionOffsets = new mutable.HashMap[TopicAndPartition, Long]()
    for (offset <- offsets.get()) {
      println(s"update offset to zk  topic:${offset.topic},partition:${offset.partition},untilOffset:${offset.untilOffset}")
      consumerPartitionOffsets.put(offset.topicAndPartition(), offset.untilOffset)
    }
    //将当前消费的offset更新到zk上
    val offsets1 = kafkaCluster.setConsumerOffsets(groupId, consumerPartitionOffsets.toMap)
    //更新offset失败
    if (offsets1.isLeft) {
      throw new SparkException("Failed to update offset to ZK")
    }
    println("====================================================================")
  }

}
