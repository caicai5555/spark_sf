package com.daoke360.task.spark_sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by ♠K on 2017-7-26.
  * QQ:272488352
  */
class GroupConcatCityUDAF extends UserDefinedAggregateFunction {

  //输入的数据类型
  override def inputSchema: StructType = {
    StructType(List(StructField("cityInfo", StringType, true)))
  }

  //每次聚合后结果的类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("bufferCityInfo", StringType, true)))
  }

  //返回结果的类型
  override def dataType: DataType = {
    StringType
  }

  //中间结果和最终结果是否一致，即数据类型是否确定
  override def deterministic: Boolean = {
    true
  }

  //聚合的初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer(0) = ""
    buffer.update(0, "")
  }

  /**
    * 定义如何进行累加
    *
    * @param buffer 累加之后的结果
    * @param input  需要累加的值
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sb = new StringBuffer()
    sb.append(buffer.getString(0) + ",")
    sb.append(input.getString(0))

    if (sb.toString.startsWith(",")) {
      buffer.update(0, sb.toString.substring(1))
    } else {
      buffer.update(0, sb.toString)
    }


  }

  /**
    * 有于我们的程序是分布式计算，那么意味着数据可能在同的节点或分区上，需要将不同节点或分区的数据进行归并
    *
    * @param buffer1 每个分区累加之后的结果
    * @param buffer2 下一个需要归并的分区
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val sb = new StringBuffer()
    sb.append(buffer1.getString(0) + ",")
    sb.append(buffer2.getString(0))

    if (sb.toString.startsWith(",")) {
      buffer1.update(0, sb.toString.substring(1))
    } else {
      buffer1.update(0, sb.toString)
    }

  }

  //返回最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}
