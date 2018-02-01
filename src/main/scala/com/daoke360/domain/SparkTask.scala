package com.daoke360.domain

/**
  * Created by ♠K on 2017-7-18.
  * QQ:272488352
  */
class SparkTask {
  var task_id: Long = 0L
  var task_param: String = null


  override def toString = s"SparkTask(task_id=$task_id, task_param=$task_param)"
}
