package com.citybike.data.processor

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ponnulingam on 6/10/19.
  */
object SparkSessionConfig {

  //Initialize the spark streaming context
  val conf = new SparkConf().setMaster("local[2]").setAppName("BikeDataStreamProcessor")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
  //TODO:
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  ssc.sparkContext.setLogLevel("ERROR")
  //TODO:

  //Kafka Configuration Params
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092", //TODO:
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "StreamProcessorGroupId",
    "auto.offset.reset" -> "latest", //TODO:
    "enable.auto.commit" -> (false: java.lang.Boolean) //TODO:
  )


}
