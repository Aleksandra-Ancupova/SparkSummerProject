package com.github.AleksandraAncupova

import org.apache.spark.sql.functions.{col, lit}

object Day23DifferentDataTypes extends App {

  val spark = SparkUtil.getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
  df.printSchema()
  df.createOrReplaceTempView("dfTable")


  df.select(lit(5), lit("five"), lit(5.0))
    spark.sql("SELECT 5, 'five', 5.0").show()

  // several options:

  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description")
    .show(5, false)


  df.where(col("InvoiceNo") === 536365)
    .select("InvoiceNo", "Description")
    .show(5, false)

  df.where("InvoiceNo = 536365")
    .show(5, false)

  spark.sql("SELECT * FROM dfTable WHERE InvoiceNo = 536365").show(5, false)

  // TODO load data from 1st of March 2011 into DF
  // get all purchases that have been made from Finland
  // sort by unit price, limit purchases to 20
  // collect results to an array of rows, print all rows

  //


}
