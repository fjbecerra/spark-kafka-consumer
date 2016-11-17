package com.pakius.helper

import java.util.Date

import org.apache.spark.rdd.RDD

/**
  * Created by FBecer01 on 21/10/2016.
  */
object Common {

  def parseDateGivenString(str : String, pattern: String) : Date = {
    val format = new java.text.SimpleDateFormat(pattern)
    format.parse(str)
  }

  def splitLine(csv: RDD[String]): RDD[Array[String]] = {
    csv.zipWithIndex filter (_._2 > 0) map (_._1.split("\t") map (elem => elem.trim))
  }
}
