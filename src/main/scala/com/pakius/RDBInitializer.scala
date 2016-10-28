package com.pakius

import java.sql.{PreparedStatement, CallableStatement, DriverManager}

import com.pakius.helper.Common
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by FBecer01 on 19/10/2016.
  */
object RDBInitializer {

  val prop = ConfigFactory.load

  def mapValuesAndExecute(values:Array[String], ps: PreparedStatement ): Unit = {
    values match {
      case user:Array[String] if user.length > 1 => {
        ps.setString(1, user(0))
        ps.setString(2, user(1))
        ps.setInt(3, if (user(2).isEmpty) -1 else user(2).toInt)
        ps.setString(4, user(3))
        ps.setDate(5, if (user(4).isEmpty) null else new java.sql.Date(Common.parseDateGivenString(user(4)).getTime))
      }
      case _ => ps.setString(1, values(0))
    }
    ps.executeUpdate
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val csv = sc.textFile("file://" + args(0))
    val data = Common.splitLine(csv)
    data.foreachPartition {
      it =>
        val conn = DriverManager.getConnection(prop.getString("rdb.url"), prop.getString("rdb.user"), prop.getString("rdb.password"))
        val del = conn.prepareStatement("INSERT INTO DIM_USERS (ID, GENDER, AGE, COUNTRY, REGISTERED) VALUES (?,?,?,?,?)")
        for (user <- it) {
          mapValuesAndExecute(user, del )
        }
    }
  }

}



