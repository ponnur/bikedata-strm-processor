package com.citybike.data.processor

import com.typesafe.config.ConfigFactory


object Config {

  val appConfiguration = ConfigFactory.load()

  val kafkaBootStrapServers = appConfiguration.getString("kafka.bootstrapservers")
  val stationStatusTopicName = appConfiguration.getString("kafka.stationstatustopicname")
  val docksAvailableFilePath = appConfiguration.getString("file.docksavailablepath")
  val avgBikeAvailableFilePath = appConfiguration.getString("file.avgbikeavlpath")
  val checkPointFilePath = appConfiguration.getString("file.checkpoint")
  val maxNoOfDocks = appConfiguration.getString("stationdata.maxnoofdocks")
  val streamInterval = appConfiguration.getInt("stationdata.streaminterval")
  val logLevel = appConfiguration.getString("stationdata.loglevel")

  override def toString: String = {
    s"""
       |KafaBootStrapServers    : ${kafkaBootStrapServers}
       |StationDataTopicName    : ${stationStatusTopicName}
       |CheckPointFilePath      : ${checkPointFilePath}
       |docksAvailableFilePath  : ${docksAvailableFilePath}
    """.stripMargin
  }
}
