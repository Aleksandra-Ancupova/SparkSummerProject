package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession

object Day18SparkSQL extends App {
  println(s"Reading CSV with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  println(s"Session started on Spark version ${spark.version}")

  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv") //relative path to our project

  println(s"We have ${flightData2015.count()} rows of data")

  flightData2015.createOrReplaceTempView("flight_data_2015")

  // now we can use SQL
  val sqlWay = spark.sql("""
  SELECT DEST_COUNTRY_NAME, count(1)
  FROM flight_data_2015
  GROUP BY DEST_COUNTRY_NAME
  """)

  // this is the other approach
  val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
//
//  sqlWay.show(10)
//  dataFrameWay.show(10)

  //TODO flight Data from 2014

  val flightData2014 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2014-summary.csv") //relative path to our project

  println(s"We have ${flightData2014.count()} rows of data")

  flightData2014.createOrReplaceTempView("flight_data_2014")

  val sqlWay2014 = spark.sql("""
  SELECT DEST_COUNTRY_NAME, count(*)
  FROM flight_data_2014
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY count(*) DESC
  """)

  // this is the other approach
  //val dataFrameWay2014 = flightData2014.groupBy("DEST_COUNTRY_NAME").count().orderBy("count").

  sqlWay2014.show(10)




}
