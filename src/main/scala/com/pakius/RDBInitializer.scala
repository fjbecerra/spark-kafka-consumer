package com.pakius

import java.sql.{DriverManager, ResultSet}
import java.util.{Date, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by FBecer01 on 19/10/2016.
  */
object RDBInitializer {

  val prop = ConfigFactory.load

  def main(args: Array[String]){
   val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

   /* val group_res = sqlContext.sql("INSERT INTO ('"+ mydate + "') as mydate, url, count(*) as cnt, sum(num_requests) as tot_visits FROM wikistats group by url")
    group_res.insertIntoJDBC(prop.getString("rdb.url"), "DIM_USERS", true);*/

    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")

    val csv = sc.textFile(getClass.getResource(args(0)).toString)

    val data = csv.zipWithIndex  filter(_._2>0)  map(_._1.split("\t")  map(elem => elem.trim))

    data.foreachPartition{
      it =>
        val conn= DriverManager.getConnection(prop.getString("rdb.url"),prop.getString("rdb.user"),prop.getString("rdb.password"))
        val del = conn.prepareStatement ("INSERT INTO DIM_USERS (ID,AGE, GENDER, COUNTRY, REGISTERED) VALUES (?,?,?,?,?)")
        for (user <-it)
        {
          del.setString(1,user.toString)
          del.setString(2,"my input")
          del.executeUpdate
        }

    }
  }


    /*r2.foreachPartition {
      it =>
        val conn= DriverManager.getConnection(url,username,password)
        val del = conn.prepareStatement ("INSERT INTO tweets (ID,Text) VALUES (?,?) ")
        for (bookTitle <-it)
        {
          del.setString(1,bookTitle.toString)
          del.setString(2,"my input")
          del.executeUpdate
        }
    }*/




   /* val spark = SparkSession
      .builder()
      .appName("Initialize dimension tables")
      .getOrCreate()

    val segments = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .load("s3n://michaeldiscenza/data/test_segments")

    val connection = new Properties
    connection.put("user",prop.getString("db.user"))
    connection.put("password",prop.getString("db.password"))
    segments.write.jdbc(prop.getString("db.jdbc"), "DIM_USER", connection)*/


  /*  val jdbcDF = spark.read
      .format("jdbc")
      .option("url", prop.getString("rdb.url"))
      .option("user", prop.getString("rdb.user"))
      .option("password", prop.getString("rdb.password"))
      .load()*/



    /*val segments = jdbcDF.sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .load("s3n://michaeldiscenza/data/test_segments")
     segments.*/



  }



    class User(id:String, gender:String,age:Int,country:String,registered:Date){}



