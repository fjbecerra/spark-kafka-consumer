package com.pakius

import java.sql.{DriverManager}

import com.pakius.common.Common
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by FBecer01 on 19/10/2016.
  */
object RDBInitializer {

  val prop = ConfigFactory.load
  val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]){

    val csv = sc.textFile("file://"+args(0))

    val data = csv.zipWithIndex  filter(_._2>0)  map(_._1.split("\t")  map(elem => elem.trim))

    data.foreachPartition{
      it =>
        val conn= DriverManager.getConnection(prop.getString("rdb.url"),prop.getString("rdb.user"),prop.getString("rdb.password"))
        val del = conn.prepareStatement ("INSERT INTO DIM_USERS (ID, GENDER, AGE, COUNTRY, REGISTERED) VALUES (?,?,?,?,?)")
        for (user <-it)
        {
          del.setString(1,user(0))
          del.setString(2,user(1))
          del.setInt(3,if(user(2).isEmpty) -1 else user(2).toInt)
          del.setString(4,user(3))
          del.setDate(5,if(user(4).isEmpty) null else new java.sql.Date(Common.parseDateGivenString(user(4)).getTime))
          del.executeUpdate
        }

    }
  }





}



