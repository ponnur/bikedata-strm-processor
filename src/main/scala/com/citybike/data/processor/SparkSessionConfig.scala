package com.citybike.data.processor

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSessionConfig {

  //Initialize the spark streaming context
  val conf = new SparkConf().setMaster("local[2]").setAppName("BikeDataStreamProcessor")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  val ssc: StreamingContext = new StreamingContext(conf, Seconds(Config.streamInterval))
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  ssc.sparkContext.setLogLevel(Config.logLevel)

  //Kafka Configuration Params
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> Config.kafkaBootStrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "StreamProcessorGroupId",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )


}
