package com.daoke360.Dao

import java.sql.ResultSet

import com.daoke360.domain.SparkTask
import com.daoke360.jdbc.JdbcManager

/**
  * Created by ♠K on 2017-7-18.
  * QQ:272488352
  */
object SparkTaskDao {

  def findTaskById(taskId: Long): SparkTask = {
    val sql = "select task_id,task_param from spark_task where task_id=?"
    var sparkTask: SparkTask = null
    val params = Array[Any](taskId)
    JdbcManager.executeQuery(sql, params, (result: ResultSet) => {
      while (result.next()) {
        sparkTask = new SparkTask
        sparkTask.task_id=result.getLong("task_id")
        sparkTask.task_param=result.getString("task_param")
      }
    })
    sparkTask
  }
}
