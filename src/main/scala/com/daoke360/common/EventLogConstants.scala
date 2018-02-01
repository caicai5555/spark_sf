package com.daoke360.common

/**
  * Created by ♠K on 2017-7-14.
  * QQ:272488352
  */
object EventLogConstants {
  /**
    * hbase 表中保存日志的列簇
    */
  val EVENT_LOGS_FAMILY_NAME = "log"
  /**
    * 日志分割符
    */
  val LOG_SEPARATOR = " "
  /**
    * 日志参数分隔符
    */
  val LOG_PARAM_SEPARATOR = "&"
  /**
    * 日志事件的名称
    */
  val LOG_COLUMN_NAME_EVENT_NAME = "en"
  /**
    * 用户的uuid
    */
  val LOG_COLUMN_NAME_UUID = "uid"
  /**
    * 用户的sessionId
    */
  val LOG_COLUMN_NAME_SID = "sid"
  /**
    * 用户所在的平台
    */
  val LOG_COLUMN_NAME_PLATFORM = "pl"
  /**
    * 用户的会员id
    */
  val LOG_COLUMN_NAME_MEMBER_ID = "mid"
  /**
    * 客户端ip
    */
  val LOG_COLUMN_NAME_IP = "ip"
  /**
    * 日志服务器时间
    */
  val LOG_COLUMN_NAME_SERVER_TIME = "s_time"
  /**
    * 用户客户端浏览器代理信息
    */
  val LOG_COLUMN_NAME_USER_AGENT = "b_usa"
  /**
    * 用户客户端浏览器代理信息
    */
  val LOG_COLUMN_NAME_URL = "url"

  /**
    * 页面标题
    */
  val LOG_COLUMN_NAME_TITLE = "tt"

  /**
    * 搜索关键词
    */
  val LOG_COLUMN_NAME_KEYWORD = "kw"

  /**
    * 浏览器名称
    */
  val LOG_COLUMN_NAME_BROWSER_NAME = "b_n"
  /**
    * 浏览器版本信息
    */
  val LOG_COLUMN_NAME_BROWSER_VERSION = "b_v"
  /**
    * 操作系统名称
    */
  val LOG_COLUMN_NAME_OS_NAME = "os_n"
  /**
    * 操作系统版本
    */
  val LOG_COLUMN_NAME_OS_VERSION = "os_v"
  /**
    * 地域信息-国家
    */
  val LOG_COLUMN_NAME_COUNTRY = "country"
  /**
    * 地域信息—省份
    */
  val LOG_COLUMN_NAME_PROVINCE = "province"
  /**
    * 地域信息-城市
    */
  val LOG_COLUMN_NAME_CITY = "city"

  /**
    * 品类
    */
  val LOG_COLUMN_NAME_CATEGORY = "c_id"

  /**
    * 商品
    */
  val LOG_COLUMN_NAME_GOODS = "gid"


  //点击了品类url标识符
  val LOG_CATEGORY_FLAG = "category.php"

  //点击了商品url标识符
  val LOG_GOODS_FLAG = "goods.php"

  //商品和品类id标识符
  val LOG_IDS_FLAG = "id="

  //商品和品类id标识符
  val LOG_SEARCH_FLAG = "search.php"

  /**
    * hbase 中保存日志的表名
    */
  val HBASE_EVENT_LOGS_TABLE = "event_log_1608"
  /**
    * hbase 表中保存日志的列簇
    */
  val HBASE_EVENT_LOGS_FAMILY_NAME = "log"
}
