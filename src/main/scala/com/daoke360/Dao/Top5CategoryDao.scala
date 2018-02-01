package com.daoke360.Dao

import com.daoke360.domain.Top5Category
import com.daoke360.jdbc.JdbcManager

import scala.collection.mutable.ListBuffer

/**
  * Created by ♠K on 2017-7-24.
  * QQ:272488352
  */
object Top5CategoryDao {

  def deleteByTaskId(taskId: Long) = {
    val sql = "delete from top5_category where task_id=?"
    val params = Array[Any](taskId)
    JdbcManager.executeUpdate(sql, params)
  }

  def insert(top5CategoryList: ListBuffer[Top5Category]) = {
    val sql = "insert into top5_category values(?,?,?,?,?)"
    val paramsList = new Array[Array[Any]](top5CategoryList.size)
    for (i <- 0 until (top5CategoryList.size)) {
      val top5Category = top5CategoryList(i)
      paramsList(i) = Array[Any](
        top5Category.taskId,
        top5Category.categoryId,
        top5Category.clickCount,
        top5Category.cartCount,
        top5Category.payCount
      )
    }
    JdbcManager.executeBatch(sql,paramsList)
  }
}
