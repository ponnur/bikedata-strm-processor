package com.citybike.data.processor

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object BikeDataProcessor extends Serializable {

  val MAX_NO_DOCS: Int = 20 //TODO:
  val conf = SparkSessionConfig.conf
  val ssc = SparkSessionConfig.ssc
  val spark =  SparkSessionConfig.spark
  val topics = Seq("station-status") //TODO: Fix Me


  val isValidPayLoad = (payLoad: String) => {
    val isValid = if (payLoad == null || payLoad.isEmpty() || !isValidJson(payLoad)) {
      false
    } else {
      true
    }

    isValid
  }

  val isValidJson = (msg: String) => {
    //TODO:
    val objectMapper = new ObjectMapper()
    objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)

    val validJson = try {
      objectMapper.readTree(msg)
      true
    } catch {
      case e: JsonProcessingException => println(e.getMessage); false //TODO:
      case e: Throwable => false
    }

    validJson
  }


  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, SparkSessionConfig.kafkaParams)
  )


  stream.foreachRDD(rdd => {
    val topicValueStrings = rdd.map(record => (record.value()).toString)
    // .filter(x => isValidJson(x))
    val df = spark.read.json(topicValueStrings)

    val timeStamp: String = currentTimeStamp()
    println("DF Count:" + df.count())

    if (df.count() > 0) {


      val DOCS_AVAILABLE_FILE_PATH = s"/tmp/city-bike-data-${timeStamp}"
      val docksPerStation: DataFrame = calculateDocksPerStation(df, timeStamp)
      saveAsCsvFile(docksPerStation, DOCS_AVAILABLE_FILE_PATH)


      val bikesAvailableDF = df.select(col("station_id"), col("num_bikes_available"))
        .groupBy("station_id")
        .agg(sum(col("num_bikes_available")).alias("num_bikes_available"))

      val avgBikeCountPath = s"/tmp/avg-city-bike-data"
      if(existingAvgBikeExists(avgBikeCountPath)){
        val avgBikeCountDF: DataFrame = readFromCsvFile(avgBikeCountPath)
        bikesAvailableDF.join(bikesAvailableDF,List("station_id")).show(400)
      } else {
        val avgBikeCountDF = bikesAvailableDF.select(col("station_id"), col("num_bikes_available").alias("avg_num_bikes_available")).withColumn("num_records_for_avg", lit(1))
        saveAsCsvFile(avgBikeCountDF, avgBikeCountPath)
      }



    } else {
      println("No data found")
    }
  })

  def processStationDataStream(): Unit = {
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def calculateDocksPerStation(df: DataFrame, timeStamp: String): DataFrame = {

    df.select(col("station_id"), col("num_docks_available"))
      .filter(col("num_docks_available") > MAX_NO_DOCS)
      .select(col("station_id")).distinct()
      .agg(count(col("station_id")).alias("NoOfStationWithOver20Docks"))
      .withColumn("TimeStamp", lit(timeStamp))
      .select(col("TimeStamp"), col("NoOfStationWithOver20Docks"))
  }


  def existingAvgBikeExists(filePath: String): Boolean = {
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
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
  }


  def readFromCsvFile(filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }

}
