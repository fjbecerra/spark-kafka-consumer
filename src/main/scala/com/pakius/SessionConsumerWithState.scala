package com.pakius

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificData
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by FBecer01 on 18/10/2016.
  */

/**
  * List of top 100 longest listening sessions (user name, length of session and when the session started)
  *  A "session" is defined as:
  *  One or more songs played by a particular user, where each song is started within 20 minutes of the previous song's start time
  */
object SessionConsumerWithState {

  val prop = ConfigFactory.load

  def main(args: Array[String]): Unit = {

      // Create context with 10 second batch interval
      val sparkConf = new SparkConf().setAppName("SessionConsumerWithState")
      val sc = new SparkContext(sparkConf)
      val sqlContext  = new  SQLContext(sc)

      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val topicsSet = prop.getString("topics").split(",").toSet
      val kafkaParams = Map[String, String](
        "auto.offset.reset" -> "smallest",
        "metadata.broker.list" -> prop.getString("brokers"),
        "schema.registry.url" -> prop.getString("schemaRegistry"))

      val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](
      ssc, kafkaParams, topicsSet)

      //Todo do staff
     // Start the computation
      val lines = messages.map(_._2.asInstanceOf[GenericRecord]) map  ( SpecificData.get().deepCopy(Event.SCHEMA$, _).asInstanceOf[Event])

      lines.foreachRDD{
        rdd => rdd.foreachPartition{
          iteration => {
            val list = iteration.toList
            val sessions = pack(list)
//Update state
            print(sessions)

          }
        }

      }



    ssc.start()
    ssc.awaitTermination()



  }

  def pack(ls: List[Event]): List[List[Event]] = {
    if (ls.isEmpty) List(List())
    else {
      val (packed, next) = ls span (x => (ls.head.getStartPlay - x.getStartPlay) < prop.getLong("session.timeout"))
      if (next == Nil) List(packed)
      else packed :: pack(next)
    }
  }


}
