package com.pakius

import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by FBecer01 on 18/10/2016.
  */
object DirectKafkaConsumer {

  val prop = ConfigFactory.load

  def main(args: Array[String]): Unit = {

      // Create context with 10 second batch interval
      val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val topicsSet = prop.getString("topics").split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getString("brokers"))
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
      //Todo do staff
     // Start the computation
      messages.print()
      ssc.start()
      ssc.awaitTermination()



  }


}
