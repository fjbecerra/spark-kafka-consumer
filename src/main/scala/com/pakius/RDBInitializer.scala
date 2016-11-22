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
  val conn = DriverManager.getConnection(prop.getString("rdb.url"), prop.getString("rdb.user"), prop.getString("rdb.password"))


  def saveUser(values:Array[String]): Unit = {
    val del = conn.prepareStatement("INSERT INTO DIM_USERS (ID, GENDER, AGE, COUNTRY, REGISTERED) VALUES (?,?,?,?,?)")

     values match {
      case user:Array[String] if user.length > 1 => {
        del.setString(1, user(0))
        del.setString(2, user(1))
        del.setInt(3, if (user(2).isEmpty) -1 else user(2).toInt)
        del.setString(4, user(3))
        del.setDate(5, if (user(4).isEmpty) null else new java.sql.Date(Common.parseDateGivenString(user(4), "MMM dd, yyyy" ).getTime))
      }
      case _ => del.setString(1, values(0))
    }
    del.executeUpdate
  }

  def createTables(): Unit ={
    conn.prepareStatement("CREATE TABLE IF NOT EXISTS DIM_USERS(ID varchar(255) NOT NULL,AGE int,GENDER enum('m','f',''),COUNTRY varchar(255),REGISTERED DATE,PRIMARY KEY (ID));") execute()
    conn.prepareStatement("CREATE TABLE IF NOT EXISTS EVENTS(ID varchar(255) NOT NULL,USER_ID varchar(255) NOT NULL,AUTHOR_ID varchar(255) NOT NULL,AUTHOR varchar(255) NOT NULL,TRACK_ID varchar(255) NOT NULL,TRACK_NAME varchar(255) NOT NULL);") execute()

  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDBInitializer")
    val sc = new SparkContext(sparkConf)
    createTables()
    val csv = sc.textFile("file://" + args(0))
    val data = Common.splitLine(csv)
    data.foreachPartition {
      it =>
        for (user <- it) {
          saveUser(user)
        }
    }
  }

}



