package com.pakius

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by FBecer01 on 18/10/2016.
  */

/**
  * List of top 100 longest listening sessions (user name, length of session and when the session started)
  *  A "session" is defined as:
  *  One or more songs played by a particular user, where each song is started within 20 minutes of the previous song's start time
  */
object SessionConsumer {

  val prop = ConfigFactory.load

  def main(args: Array[String]): Unit = {

      // Create context with 10 second batch interval
      val sparkConf = new SparkConf().setAppName("SessionConsumer")
      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val topicsSet = prop.getString("topics").split(",").toSet
      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> prop.getString("brokers"),
        "schema.registry.url" -> prop.getString("schemaRegistry"))
      val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](
      ssc, kafkaParams, topicsSet)
      //Todo do staff
     // Start the computation
     /* messages.mapPartitions{
        (iterator)=> {
          val list = iterator.toList
          list match {
            case  head => head::
          }
        }
      }*/
    ssc.start()
    ssc.awaitTermination()



  }

  def findSessions(list : List[(Object, Event)]) : List[(Object, Event)] = {
    def findSessionAcc(list: List[(Object, Event)], acc: List[(Object, Event)]): List[(Object, Event)] = list match {

      case Nil => Nil                                                               //prop.getLong("session.timeout")
      case head :: tail => if ((head._2.getStartPlay - tail.head._2.getStartPlay) < 1000) {
        findSessionAcc(tail, head :: acc)
      } else {
        findSessionAcc(tail, acc)
      }

    }
    findSessionAcc(list, Nil)

  }


}
