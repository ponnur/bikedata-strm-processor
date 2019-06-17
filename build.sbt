import sbt.Keys._
import sbt._

name := "bikedata-strm-processor"

scalaVersion := "2.12.1"

val sparkVersion = "2.4.0"

assemblyJarName in assembly := "bikedata-strm-processor.jar"

libraryDependencies ++= Seq(
  "com.typesafe"                    % "config"                        % "1.2.0",
  "commons-io"                      % "commons-io"                    % "2.3",
  "com.fasterxml.jackson.module"    %% "jackson-module-scala"         % "2.9.9",
  "org.apache.spark"                %  "spark-streaming_2.12"         % sparkVersion,
  "org.apache.spark"                %% "spark-core"                   % sparkVersion,
  "org.apache.spark"                %% "spark-sql"                    % sparkVersion,
  "org.apache.spark"                %% "spark-streaming-kafka-0-10"   % "2.4.3",
  "org.apache.spark"                %% "spark-sql-kafka-0-10"         % "2.4.3",
  "com.typesafe"                    % "config"                        % "1.2.0"
)

//Test dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP5" % Test
libraryDependencies += "org.mockito" % "mockito-all" % "1.10.19" % Test


assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)

