#!/usr/bin/env bash

pwd=${PWD##*/}
sbt clean assembly
mkdir ./assembly
#cp -rf target/scala-2.12/*.jar ./assembly

