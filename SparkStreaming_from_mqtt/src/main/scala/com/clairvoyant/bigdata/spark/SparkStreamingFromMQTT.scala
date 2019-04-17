package com.clairvoyant.bigdata.spark


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object SparkStreamingFromMQTT {

  // Load values form the Config file(application.json)
  val config: Config = ConfigFactory.load("application.json")

  val SPARK_APP_NAME: String = config.getString("spark.app_name")
  val SPARK_MASTER: String = config.getString("spark.master")

  val MQTT_BROKER_URL: String = config.getString("mqtt.broker_url")

  val LOGGER: Logger = LoggerFactory.getLogger(SparkStreamingFromMQTT.getClass.getName)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val lines = MQTTUtils.createPairedStream(ssc, MQTT_BROKER_URL, Array("+/+/+/pushers"), StorageLevel.MEMORY_ONLY_SER_2)

    lines.foreachRDD(rdd => {

      print("\nMessage: " + rdd.toString())
      print(" No of Messages: " + rdd.count())

      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      var dF = rdd.toDF()
      dF = dF.withColumnRenamed("_2", "value")
      dF.show()
      dF.printSchema()
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
