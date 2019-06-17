package com.citybike.data.processor

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.citybike.data.processor.Config.{docksAvailableFilePath, checkPointFilePath}
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._


object BikeDataProcessor extends Serializable {

  val conf = SparkSessionConfig.conf
  val ssc: StreamingContext = SparkSessionConfig.ssc
  val spark: SparkSession = SparkSessionConfig.spark
  val topics: Seq[String] = Seq(Config.stationStatusTopicName)
  val ROUNDOFF_PRECISION = 2

  //DataFrame Column Names
  val STATION_ID: String = "station_id"
  val NO_BIKES_AVAILABLE: String = "num_bikes_available"
  val NO_RECORDS_FOR_AVG: String = "num_records_for_avg"
  val AVG_NO_OF_BIKS_AVAILABLE: String = "avg_" + NO_BIKES_AVAILABLE
  val UPDATED_AVG_NO_BIKES_AVAIL: String = "updated_avg_" + NO_BIKES_AVAILABLE
  val UPDATED_NO_RECORDS_FOR_AVG: String = "updated_" + NO_RECORDS_FOR_AVG
  val TIME_STAMP: String = "time_stamp"


  val isValidPayLoad: (String) => Boolean = (payLoad: String) => {
    val isValid = if (payLoad == null || payLoad.isEmpty() || !isValidJson(payLoad)) false else true
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

  val saveAsCsvFile = (df: DataFrame, filePath: String) => {
    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true").format("csv")
      .save(filePath)
  }


  val saveCheckPointState = (df: DataFrame) => {
    val checkPointStateDF = df.select(col(STATION_ID), col(AVG_NO_OF_BIKS_AVAILABLE), col(NO_RECORDS_FOR_AVG)).orderBy(col(STATION_ID))
    df.withColumn("type", lit("saveCheckPointState")).show(10, false)
    df.show()
    saveAsCsvFile(checkPointStateDF, checkPointFilePath)
  }


  val processStationDataStream = () => {
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  val calculateDocksPerStation = (df: DataFrame, timeStamp: String) => {

    df.select(col(STATION_ID), col("num_docks_available"))
      .filter(col("num_docks_available") > Config.maxNoOfDocks)
      .select(col(STATION_ID)).distinct()
      .agg(count(col(STATION_ID)).alias("NoOfStationWithOver20Docks"))
      .withColumn(TIME_STAMP, lit(timeStamp))
      .select(col(TIME_STAMP), col("NoOfStationWithOver20Docks"))
  }

  val existingAvgBikeExists = (filePath: String) => {
    import java.nio.file.{Paths, Files}
    val path = new Path(filePath)
    Files.exists(Paths.get(filePath))
  }

  val currentTimeStamp = () => {
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
  }


  val readChkPtDataFromCsvFile = (filePath: String) => {
    spark.read.option("header", "true")
      .option("schema", s"${STATION_ID}: string ,${AVG_NO_OF_BIKS_AVAILABLE}: double ,${NO_RECORDS_FOR_AVG}: long")
      .csv(filePath)
  }

  val saveAvgAvailableBike = (df: DataFrame, ts: String) => {
    val avgAvlBikeDF = df.withColumn(TIME_STAMP, lit(ts)).select(col(TIME_STAMP), col(STATION_ID), col(AVG_NO_OF_BIKS_AVAILABLE)).orderBy(col(STATION_ID))
    saveAsCsvFile(avgAvlBikeDF, Config.avgBikeAvailableFilePath + "-" + ts)
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
    println(s"${timeStamp} DF Count:" + df.count())

    if (df.count() > 0) {

      //Calculate the docksPerStation Metrics
      val DOCS_AVAILABLE_FILE_PATH = s"${docksAvailableFilePath}-${timeStamp}"
      val docksPerStation: DataFrame = calculateDocksPerStation(df, timeStamp)
      saveAsCsvFile(docksPerStation, DOCS_AVAILABLE_FILE_PATH)

      //Calculate the average bikes available metrics
      val bikesAvailableDF = df.select(col(STATION_ID), col(NO_BIKES_AVAILABLE)).groupBy(STATION_ID)
        .agg(avg(col(NO_BIKES_AVAILABLE)).alias(NO_BIKES_AVAILABLE))

      val bikeAvailabeWithAgvDF = calculateAvgNoOfBikesAvailable(bikesAvailableDF)
      bikeAvailabeWithAgvDF.show(10)
      saveAvgAvailableBike(bikeAvailabeWithAgvDF, timeStamp)

      //Save the current state for the next run
      saveCheckPointState(bikeAvailabeWithAgvDF)

    } else {
      println(s"${timeStamp} No data found")
    }
  })


  val calculateAvgNoOfBikesAvailable = (df: DataFrame) => {
    val avgBikeCountDF: DataFrame = existingAvgBikeExists(checkPointFilePath) match {
      case true => {

        val previousAvgBikeCountDF: DataFrame = readChkPtDataFromCsvFile(checkPointFilePath).cache()
        val dfWithPrevAvg = df.join(previousAvgBikeCountDF, List(STATION_ID), "full_outer")

        dfWithPrevAvg
          .withColumn(UPDATED_AVG_NO_BIKES_AVAIL,
                when(col(NO_BIKES_AVAILABLE).isNull, col(AVG_NO_OF_BIKS_AVAILABLE)).when(col(AVG_NO_OF_BIKS_AVAILABLE).isNull, col(NO_BIKES_AVAILABLE))
            .otherwise(lit(col(AVG_NO_OF_BIKS_AVAILABLE)) + lit(lit(col(NO_BIKES_AVAILABLE) - col(AVG_NO_OF_BIKS_AVAILABLE)) / lit(col(NO_RECORDS_FOR_AVG)))))

          .withColumn(UPDATED_NO_RECORDS_FOR_AVG, when(col(NO_BIKES_AVAILABLE).isNull, col(NO_RECORDS_FOR_AVG)).when(col(NO_RECORDS_FOR_AVG).isNull, lit(1L))
            .otherwise(col(NO_RECORDS_FOR_AVG) + 1L))

          .select(col(STATION_ID), col(UPDATED_AVG_NO_BIKES_AVAIL).as(AVG_NO_OF_BIKS_AVAILABLE), col(UPDATED_NO_RECORDS_FOR_AVG).as(NO_RECORDS_FOR_AVG))

      }
      case false => {
        df.select(col(STATION_ID), col(NO_BIKES_AVAILABLE).alias(AVG_NO_OF_BIKS_AVAILABLE)).withColumn(NO_RECORDS_FOR_AVG, lit(1))
      }
    }
    avgBikeCountDF
  }


}
