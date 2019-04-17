package com.clairvoyant.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence


object MQTTPublisher {

  // Load values form the Config file(application.json)
  val config: Config = ConfigFactory.load("application.json")

  val MQTT_BROKER_URL: String = config.getString("mqtt.broker_url")
  println(MQTT_BROKER_URL + "")

  val MQTT_TOPIC: Array[AnyRef] = config.getList("mqtt.topic").unwrapped().toArray()

  def main(args: Array[String]) {
    var client: MqttClient = null

    try {
      val persistence = new MemoryPersistence()
      client = new MqttClient(MQTT_BROKER_URL, MqttClient.generateClientId(), persistence)
      client.connect()

      val msgContent = "hello mqtt demo for spark streaming"
      val message = new MqttMessage(msgContent.getBytes("utf-8"))

      val msg: String = null

      while (true) {
        try {

          MQTT_TOPIC.foreach(rdd => {
            println("getting all topics name-->" + rdd)
            val msg = client.getTopic(rdd.toString)
            msg.publish(message)
            println(s"Published data msg: ${msg.getName}; Message: ${message}")
          })

        } catch {
          case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_MAX_INFLIGHT =>
            Thread.sleep(10)
            println("Queue is full, wait for to consume data from the message queue")
        }
      }
    } catch {
      case e: MqttException => println("Exception Caught: " + e)
    } finally {
      if (client != null) {
        client.disconnect()
      }
    }
  }
}

