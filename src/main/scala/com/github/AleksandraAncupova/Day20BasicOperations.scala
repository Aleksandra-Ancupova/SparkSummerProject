package com.github.AleksandraAncupova

import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}

object Day20BasicOperations extends App {
  println("Chapter 5")
  val spark = SparkUtil.getSpark("BasicSpark")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)
  df.show(5)
  println(df.schema)

  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  println(df.columns.mkString(","))

  val firstRow = df.first()
  println(firstRow)
  val lastRow = df.tail(1)(0)
  println(lastRow)

  // before writing we need to go back to one partition
  df.coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv("src/resources/csv/flight_summary_2015")

}
