package com.clairvoyant.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

object MQTTPublisher {

  // Load values form the Config file(application.json)
  val config: Config = ConfigFactory.load("application.json")

  val MQTT_TOPIC: String = config.getString("mqtt.topic")
  val MQTT_BROKER_URL: String = config.getString("mqtt.broker_url")

  def main(args: Array[String]) {
    var client: MqttClient = null

    try {
      val persistence = new MemoryPersistence()
      client = new MqttClient(MQTT_BROKER_URL, MqttClient.generateClientId(), persistence)

      client.connect()

      val msgtopic_pushers = client.getTopic("malaysia/1000/1234/pushers")
      val msgContent_pushers = "hello mqtt demo for spark streaming pushers malaysia"

      val msgtopic_pressure_cycle = client.getTopic("china/1000/1234/pushers")
      val msgContent_pressure_cycle = "hello mqtt demo for spark streaming pushers china"

      val msgtopic_DeviceADCs = client.getTopic("singapore/1000/1234/pushers")
      val msgContent_DeviceADCs = "hello mqtt demo for spark streaming pushers singapore"

      val message_pushers = new MqttMessage(msgContent_pushers.getBytes("utf-8"))
      val message_pressure_cycle = new MqttMessage(msgContent_pressure_cycle.getBytes("utf-8"))
      val message_DeviceADCs = new MqttMessage(msgContent_DeviceADCs.getBytes("utf-8"))

      while (true) {
        try {
          msgtopic_pushers.publish(message_pushers)
          println(s"Published data. topic: ${msgtopic_pushers.getName}; Message: $message_pushers")

          msgtopic_pressure_cycle.publish(message_pressure_cycle)
          println(s"Published data. topic: ${msgtopic_pressure_cycle.getName}; Message: $message_pressure_cycle")

          msgtopic_DeviceADCs.publish(message_DeviceADCs)
          println(s"Published data. topic: ${msgtopic_DeviceADCs.getName}; Message: $message_DeviceADCs")

          //Thread.sleep(10000)

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

