package com.pakius


import com.pakius.services.KafkaBroker
import com.typesafe.config.ConfigFactory

import scala.io.{BufferedSource, Source}


/**
 * Created by fbecer01 on 26/10/16.
 */

/**
 * Simple scala app to produce messages from files
 */
object EventPlayMusicPublisher {

  val prop = ConfigFactory.load

  def splitLines(playListFile: BufferedSource) : Vector[Vector[String]] = {
    playListFile.getLines().map(_.split(",").toVector).toVector
  }

  def main(args: Array[String]): Unit = {

      val playListFile = Source.fromFile(args(0))
      val plays = splitLines(playListFile)
      playListFile.close
      plays.foreach( str => KafkaBroker.sendMessage(prop.getString("topic"), str))
      //TODO set up logging
      System.out.println("Sent 50 Events to Kafka")
  }


}
