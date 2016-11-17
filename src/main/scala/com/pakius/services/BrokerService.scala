package com.pakius.services


import java.util.Properties

import com.pakius.Event
import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
 * Created by fbecer01 on 27/10/16.
 */
trait BrokerService {
  def sendMessage(topic:String, msg: Event)
}

case class KafkaService(brokers:String, schemaRegistry: String) extends BrokerService{

  val setupBroker: Properties={
    // Zookeeper connection properties
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("schema.registry.url",schemaRegistry)
    props
  }

 // val config = new ProducerConfig(setupBroker)
  val producer = new KafkaProducer[String, Event](setupBroker)

  def sendMessage(topic:String, msg: Event) = {
  //  val message = new KeyedMessage[String, Event](topic,null, msg)
    val message =  new ProducerRecord[String, Event](topic, null, msg);
    producer.send(message)
  }
}

object KafkaBroker {

  val prop = ConfigFactory.load

  val kafka: BrokerService = new KafkaService(prop.getString("brokers"), prop.getString("schemaRegistry"))

  def sendMessage(topic:String, msg: Event) = {
    kafka.sendMessage(topic, msg)
  }
}

