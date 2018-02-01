package com.daoke360.domain

import com.daoke360.common.GlobalConstants

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ♠K on 2017-8-1.
  * QQ:272488352
  */
class LocationDimension(var country: String, var province: String, var city: String) extends BaseDimension {
  var id: Int = 0

  def canEqual(other: Any): Boolean = other.isInstanceOf[LocationDimension]

  override def equals(other: Any): Boolean = other match {
    case that: LocationDimension =>
      (that canEqual this) &&
        this.id == that.id &&
        this.country.equals(that.country) &&
        this.province.equals(that.province) &&
        this.city.equals(that.city)
    case _ => false
  }
}

object LocationDimension {

  /**
    * 中国    all       all
    * 中国    河北     all
    * 中国    河北    唐山
    */
  def buildList(country: String, province: String, city: String) = {
    val buffer = ArrayBuffer[LocationDimension]()
    buffer.append(new LocationDimension(country, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL))
    buffer.append(new LocationDimension(country, province, GlobalConstants.VALUE_OF_ALL))
    buffer.append(new LocationDimension(country, province, city))
    buffer
  }

}