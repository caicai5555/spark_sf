package com.daoke360.task.spark_rdd.session

/**
  * Created by ♠K on 2017-7-24.
  * QQ:272488352
  */
class CategorySortKey(var clickCount: Long, var cartCount: Long, var payCount: Long) extends
  Ordered[CategorySortKey] with Serializable {
  override def compare(that: CategorySortKey): Int = {
    if (this.clickCount - that.clickCount != 0) (this.clickCount - that.clickCount).toInt
    else if (this.cartCount - that.cartCount != 0) (this.cartCount - that.cartCount).toInt
    else (this.payCount - that.payCount).toInt
  }

  override def toString = s"CategorySortKey(clickCount=$clickCount, cartCount=$cartCount, payCount=$payCount)"
}
