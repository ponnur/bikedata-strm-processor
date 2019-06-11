package com.citybike.data.processor

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql.functions._


class BikeDataProcessorSpec extends FunSpec {
  implicit lazy val sparkSession: SparkSession = {
    SparkSession.builder().master("local").appName("WhatifanalysisUnitTest").getOrCreate()
  }
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._



  describe("local path") {

    it("should calculate the running average of the bikes available"){

      val avgBikeCountDF = Seq(
        ("station_1", 10.4, 10),
        ("station_2", 20.25, 4))
        .toDF("station_id", "avg_num_bikes_available", "num_records_for_avg")


    }

    it("should read and write to csv") {

      val bikeCountDF = Seq(
        ("station_1", 100L, 30),
        ("station_2", 150L, 45))
        .toDF("station_id", "total_count", "no_readings")

      bikeCountDF.show()

      val path = "/Users/ponnulingam/ns/city_bike/bikedata-strm-processor/bike_data_checkpoint"


      bikeCountDF.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true").format("csv")
        .save(path)


      val updatedBikeCountDF = Seq(
        ("station_1", 50L, 1),
        ("station_2", 60L, 1),
        ("station_4", 20L, 2))
        .toDF("station_id", "total_count", "no_readings")


      val sourceDF = sparkSession.read.option("header","true").csv(path)

      sourceDF.cache()

      val updatedDF = sourceDF
        .union(updatedBikeCountDF)

      updatedDF.show()

      val mergedDF: DataFrame = updatedDF.groupBy(col("station_id")).agg(sum(col("total_count")).alias("total_count"), sum(col("no_readings")).alias("no_readings"))

      mergedDF.show()

      mergedDF.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true").format("csv")
        .save(path)


      sparkSession.read.option("header","true").csv(path).show()

    }


  }


}
