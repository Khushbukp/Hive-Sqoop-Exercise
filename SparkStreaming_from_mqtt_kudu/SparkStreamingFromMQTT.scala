package com.clairvoyant.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object SparkStreamingFromMQTT {

  // Load values form the Config file(application.json)
  val config: Config = ConfigFactory.load("application.json")

  val SPARK_APP_NAME: String = config.getString("spark.app_name")
  val SPARK_MASTER: String = config.getString("spark.master")
  val SPARK_WAREHOUSE: String = config.getString("spark.warehouse")

  val MQTT_TOPIC: String = config.getString("mqtt.topic")
  val MQTT_BROKER_URL: String = config.getString("mqtt.broker_url")

//  val HIVE_DATABASE: String = config.getString("hive.database_name")
//  val HIVE_TABLE: String = config.getString("hive.table_name")

  val KUDU_MASTER: String = config.getString("kudu.master")
  val KUDU_TABLE: String = config.getString("kudu.table_name")

  val LOGGER: Logger = LoggerFactory.getLogger(SparkStreamingFromMQTT.getClass.getName)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME)

    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val sc = ssc.sparkContext
    val kuduContext = new KuduContext(KUDU_MASTER, sc)

//    ssc.sparkContext.setLogLevel("ERROR")
    val lines = MQTTUtils.createPairedStream(ssc, MQTT_BROKER_URL, Array(MQTT_TOPIC), StorageLevel.MEMORY_ONLY_SER_2)

    lines.foreachRDD(rdd => {

      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._

      print("\nMessage: " + rdd.foreach(x => print(x + "\n")))
      print(" No of Messages: " + rdd.count())

      var dF = rdd.toDF()
      dF = dF.withColumnRenamed("_1","key").withColumnRenamed("_2", "value")
      dF = dF.withColumn("guid", monotonically_increasing_id())
      dF = dF.select("guid","key","value")
      dF.show()

//      dF.write.mode("overwrite").insertInto(HIVE_DATABASE + "." + HIVE_TABLE)
      kuduContext.upsertRows(dF,"impala::" + KUDU_TABLE)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
