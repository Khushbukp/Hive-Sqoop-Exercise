package com.clairvoyant.bigdata.spark

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.FileUtils

object MQTTSubscriber {
  def main(args: Array[String]) {

    // val checkpoint = "/Users/khushbu/Dummy2/chk/"
    // val data = "/Users/khushbu/Dummy2/data/"
    val checkpoint = "hdfs://hadoop31.clairvoyant.local:8020/user/clairvoyant/khushbu/chk/"
    // val data = "hdfs://hadoop31.clairvoyant.local:8020/user/clairvoyant/khushbu/data/"
    FileUtils.deleteDirectory(new File(checkpoint))
    // FileUtils.deleteDirectory(new File (data))

    val spark = SparkSession
      .builder
      .appName("Structured-Streaming")
      //.master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN");
    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to mqtt server
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", "+/+/+/pushers")
      .option("persistence", "memory")
      .load("tcp://10.10.4.76:1883")
      .selectExpr("CAST(topic AS STRING)", "CAST(payload AS STRING)")
      .as[(String, String)]

    println(lines.isStreaming)
    println(lines.printSchema)

    val query = lines.writeStream
      .format("csv")
      .option("path", "hdfs://hadoop31.clairvoyant.local:8020/user/clairvoyant/khushbu/data/")
      .option("checkpointLocation", "/user/clairvoyant/khushbu/chk/")
//      .option("path", "/Users/khushbu/Dummy2/data/")
//      .option("checkpointLocation", "/Users/khushbu/Dummy2/chk/")
      .start()

    query.awaitTermination()
  }
}
