package com.daoke360.Dao

import com.daoke360.domain.SessionAggrStat
import com.daoke360.jdbc. JdbcManager

/**
  * Created by ♠K on 2017-7-20.
  * QQ:272488352
  */
object SessionAggrStatDao {


  def deleteByTaskId(taskId: Long) = {
    val sql = "delete from  session_aggr_stat  where task_id=?"
    val params = Array[Any](taskId)
    JdbcManager.executeUpdate(sql, params)
  }

  def insert(sessionAggrStat: SessionAggrStat): Unit = {
    val sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val params = Array[Any](
      sessionAggrStat.taskid,
      sessionAggrStat.session_count,
      sessionAggrStat.visit_length_1s_3s_ratio,
      sessionAggrStat.visit_length_4s_6s_ratio,
      sessionAggrStat.visit_length_7s_9s_ratio,
      sessionAggrStat.visit_length_10s_30s_ratio,
      sessionAggrStat.visit_length_30s_60s_ratio,
      sessionAggrStat.visit_length_1m_3m_ratio,
      sessionAggrStat.visit_length_3m_10m_ratio,
      sessionAggrStat.visit_length_10m_30m_ratio,
      sessionAggrStat.visit_length_30m_ratio,
      sessionAggrStat.step_length_1_3_ratio,
      sessionAggrStat.step_length_4_6_ratio,
      sessionAggrStat.step_length_7_9_ratio,
      sessionAggrStat.step_length_10_30_ratio,
      sessionAggrStat.step_length_30_60_ratio,
      sessionAggrStat.step_length_60_ratio
    )
    JdbcManager.executeUpdate(sql, params)
  }


}
