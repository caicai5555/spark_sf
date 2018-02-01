package com.daoke360.Dao

import com.daoke360.domain.AreaTop3Product
import com.daoke360.jdbc.JdbcManager

/**
  * Created by ♠K on 2017-7-27.
  * QQ:272488352
  */
object AreaTop3ProductDao {


  def deleteByTaskId(taskId: Long) = {
    val sql = "delete from area_top3_product where task_id=?"
    val params = Array[Any](taskId)
    JdbcManager.executeUpdate(sql, params)
  }

  def insert(arrayList: Array[AreaTop3Product]) = {
    val sql = "insert into area_top3_product values(?,?,?,?,?,?)"

    val params = new Array[Array[Any]](arrayList.size)
    for (i <- 0 until (params.length)) {
      params(i) = Array[Any](
        arrayList(i).taskId,
        arrayList(i).area,
        arrayList(i).productId,
        arrayList(i).city_infos,
        arrayList(i).clickCount,
        arrayList(i).productName
      )
    }
    JdbcManager.executeBatch(sql, params)
  }


  def main(args: Array[String]): Unit = {
    deleteByTaskId(2)
  }
}
