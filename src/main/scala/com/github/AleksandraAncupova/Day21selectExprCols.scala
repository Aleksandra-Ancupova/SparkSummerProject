package com.github.AleksandraAncupova

import org.apache.spark.sql.functions.{expr, lit}

object Day21selectExprCols extends App {
  val spark = SparkUtil.getSpark("Sparky")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  // so auto detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(3)
  val statDf = df.describe()  //shows basic stats on string and numeric columns
  statDf.show()

  df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

  df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    .show(5)

  df.where("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show()  // should show only one

  df.select(expr("*"), lit(42).as("The answer")).show(5)

  df.withColumn("numberOne", lit(1))
    .withColumn("numberTwo", lit(2))
    .show(3)

  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .withColumn("BigCount", expr("count *100 + 10000"))
    .show(5)

  // renaming column
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

  // `  - backtick symbol is used for reserved characters
}
