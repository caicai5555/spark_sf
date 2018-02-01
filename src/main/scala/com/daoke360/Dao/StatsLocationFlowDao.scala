package com.daoke360.Dao

import java.sql.Date

import com.daoke360.domain.StatsLocationFlow
import com.daoke360.jdbc.JdbcManager

import scala.collection.mutable.ListBuffer

/**
  * Created by ♠K on 2017-8-2.
  * QQ:272488352
  */
object StatsLocationFlowDao {

  def updatePv(listBuffer: ListBuffer[StatsLocationFlow]) = {
    val sql = "insert into stats_location_flow(date_dimension_id,location_dimension_id,pv,created)values(?,?,?,?)on duplicate key update pv=pv+?"
    val paramList = new Array[Array[Any]](listBuffer.length)
    for (i <- 0 until (listBuffer.length)) {
      paramList(i) = Array[Any](
        listBuffer(i).date_dimension_id,
        listBuffer(i).location_dimension_id,
        listBuffer(i).pv,
        new Date(listBuffer(i).created.getTime),
        listBuffer(i).pv
      )
    }
    JdbcManager.executeBatch(sql, paramList)
  }


  def setOrUpdateFlow(listBuffer: ListBuffer[StatsLocationFlow]) = {
    val sql =
      """
        |insert into stats_location_flow(date_dimension_id,location_dimension_id,nu,uv,pv,sn,`on`,created)
        |values(?,?,?,?,?,?,?,?)on duplicate key update nu=?,uv=?,pv=?,sn=?,`on`=?
      """.stripMargin
    val paramList = new Array[Array[Any]](listBuffer.length)
    for (i <- 0 until (listBuffer.length)) {
      paramList(i) = Array[Any](
        listBuffer(i).date_dimension_id,
        listBuffer(i).location_dimension_id,
        listBuffer(i).nu,
        listBuffer(i).uv,
        listBuffer(i).pv,
        listBuffer(i).sn,
        listBuffer(i).on,
        new Date(listBuffer(i).created.getTime),
        listBuffer(i).nu,
        listBuffer(i).uv,
        listBuffer(i).pv,
        listBuffer(i).sn,
        listBuffer(i).on
      )
    }
    JdbcManager.executeBatch(sql, paramList)
  }

}
