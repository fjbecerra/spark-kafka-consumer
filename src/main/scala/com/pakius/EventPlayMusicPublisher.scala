package com.pakius



import com.pakius.helper.Common
import com.pakius.services.{BrokerService, KafkaBroker, KafkaService}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}



/**
 * Created by fbecer01 on 26/10/16.
 */

/**
 * Simple scala app to produce messages from files
 */
object EventPlayMusicPublisher {

  val prop = ConfigFactory.load

  val kafka: BrokerService = new KafkaService(prop.getString("brokers"), prop.getString("schemaRegistry"))

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EventPlayMusicPublisher")
    val sc = new SparkContext(sparkConf)
    val csv = sc.textFile("file://" + args(0))
    val data = Common.splitLine(csv)
    data.foreachPartition {
      it =>
        it.foreach(
          str => kafka.sendMessage(
            prop.getString("topics"), AvroConverter.Event(str)
          )
        )
    }
  }



}
