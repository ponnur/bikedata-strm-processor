package com.citybike.data.processor


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{col, sum, udf, _}


case class StationVehicleCount(stationId:String, count: Long, noOfReadings: Long)
case class StationVehicleUpdate(stationId:String, count: Long, noOfReadings: Long)
case class VehicleCount(count: Long)

object BikeDataStreamProcessor /* extends App */ {
  val MAX_NO_DOCS: Int = 20


  val DOCS_AVAILABLE_FILE_PATH = "/Users/ponnulingam/ns/city_bike/bikedata-strm-processor"

  println("Starting the bike data loader applcation")

  val conf = new SparkConf().setMaster("local[2]").setAppName("BikeDataStreamProcessor")
              .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  ssc.sparkContext.setLogLevel("ERROR")


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "StreamProcessorGroupId",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Seq("station-status")

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  val x = stream.foreachRDD(rdd => {
    val topicValueStrings = rdd.map(record => (record.value()).toString)
    val df = spark.read.json(topicValueStrings)

    println("DF Count:" + df.count())
    if(df.count() > 0) {

      val timeStamp: String = currentTimeStamp()
      val docksCountDF: DataFrame = df.select(col("station_id"),col("num_docks_available"))
                                                            .filter(col("num_docks_available") > MAX_NO_DOCS)
                                                            .select(col("station_id")).distinct()
                                                            .agg(count(col("station_id")).alias("NoOfStationWithOver20Docks"))
                                                            .withColumn("TimeStamp", lit(timeStamp))
                                                            .select(col("TimeStamp"), col("NoOfStationWithOver20Docks"))


      val bikesAvailableDF = df.select(col("station_id"), col("num_bikes_available"))
                                        .groupBy("station_id")
                                        .agg(sum(col("num_bikes_available").alias("num_bikes_available")))

      val avgBikeCountPath = ""

      if(existingAvgBikeExists(avgBikeCountPath)){
        val avgBikeCountDF: DataFrame = readFromCsvFile(avgBikeCountPath)
        bikesAvailableDF.join(bikesAvailableDF,col("station_id")).show(10)

      } else {
        val avgBikeCountDF = bikesAvailableDF.select(col("station_id"), col("num_bikes_available").alias("avg_num_bikes_available")).withColumn("num_records_for_avg", lit(1))
        saveAsCsvFile(avgBikeCountDF, avgBikeCountPath)
      }


      val filePath= s"${DOCS_AVAILABLE_FILE_PATH}/Over20DocksAvailable_${timeStamp}"
      saveAsCsvFile(docksCountDF,filePath)

    } else {
      //TODO: Log it as error
    }

  })


  def existingAvgBikeExists(filePath: String): Boolean = {
    //Check if file exists
    import java.nio.file.{Paths, Files}
    val path = new Path(filePath)
    Files.exists(Paths.get(filePath))
  }


  def saveAsCsvFile(df: DataFrame, filePath: String) = {
    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true").format("csv")
      .save(filePath)
  }


  def currentTimeStamp(): String = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").format(LocalDateTime.now)
  }


  def readFromCsvFile(filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }



}
