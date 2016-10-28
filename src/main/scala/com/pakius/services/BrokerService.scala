package com.pakius.services


import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


/**
 * Created by fbecer01 on 27/10/16.
 */
trait BrokerService {
  def sendMessage(topic:String, msg: String)
}

case class KafkaService(brokers:String) extends BrokerService{

  val setupBroker: Properties={
    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class","kafka.serializer.StringEncoder")
    props
  }

  val config = new ProducerConfig(setupBroker)
  val producer = new Producer[String, String](config)

  def sendMessage(topic:String, msg: String) = {
    val message = new KeyedMessage[String, String](topic, null, msg)
    producer.send(message)
  }
}

object KafkaBroker {

  val prop = ConfigFactory.load

  val kafka: BrokerService = new KafkaService(prop.getString("brokers"))

  def sendMessage(topic:String, msg: String) = {
    kafka.sendMessage(topic, msg)
  }
}

