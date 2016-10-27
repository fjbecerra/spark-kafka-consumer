package com.pakius.services


import java.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}


/**
 * Created by fbecer01 on 27/10/16.
 */
trait BrokerService {
  def sendMessage(topic:String, msg: Any)
}

case class KafkaService(brokers:String) extends BrokerService{

    val setupBroker: util.HashMap[String, Object] ={
      // Zookeeper connection properties
      val props = new util.HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      props
    }

    val producer = new KafkaProducer[String, Any](setupBroker)

    def sendMessage(topic:String, msg: Any) = {
      val message = new ProducerRecord[String, Any](topic, null, msg)
      producer.send(message)
    }
}

object KafkaBroker {

  val prop = ConfigFactory.load

  val kafka: BrokerService = new KafkaService(prop.getString("brokers"))

  def sendMessage(topic:String, msg: Any) = {
    kafka.sendMessage(topic, msg)
  }
}
