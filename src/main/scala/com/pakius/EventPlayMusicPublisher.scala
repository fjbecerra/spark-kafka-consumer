package com.pakius


import com.pakius.helper.Common
import com.pakius.services.KafkaBroker
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}


/**
 * Created by fbecer01 on 26/10/16.
 */

/**
 * Simple scala app to produce messages from files
 */
object EventPlayMusicPublisher {

  val prop = ConfigFactory.load


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val csv = sc.textFile("file://" + args(0))
    val data = Common.splitLine(csv)
    data.foreachPartition {
      it =>
        it.foreach(str => KafkaBroker.sendMessage(prop.getString("topic"), str.toString ))

    }
  }



}
