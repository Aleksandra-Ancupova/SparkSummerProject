package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, column, desc, col}

object Day19StreamingExample extends App {

  println(s"Exploring Streaming with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  // in Scala
  //csv reading documentation: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
  val staticDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true") //we are letting Spark figure out the type of data in our csvs
    //    .csv("src/resources/retail-data/by-day/2010-12-01.csv")
    .csv("src/resources/retail-data/by-day/*.csv") //in order for this to work Hadoop (via Wintils on Windows) should be installed
  //get Hadoop with Winutils for Windows from https://github.com/kontext-tech/winutils
  //src/resources/retail-data/by-day
  //    .load("src/resources/retail-data/by-day/*.csv") //notice the wildcard we are loading everything!
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  println(staticSchema.toArray.mkString("\n"))

  println(s"We got ${staticDataFrame.count()} rows of data!") //remember count action goes across all partitions

    val streamingDataFrame = spark.readStream
      .schema(staticSchema) //we provide the schema that we got from our static read
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
  //    .load("src/resources/retail-data/by-day/*.csv")
      .load("src/resources/retail-data/by-day/2010-12-01.csv")


    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  //    .groupBy(
  //      col("CustomerId"), window(col("CustomerId"), "1 day"))
      .sum("total_cost")


purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()

  // in Scala
  spark.sql("""
      SELECT *
      FROM customer_purchases
      ORDER BY `sum(total_cost)` DESC
      """)
    .show(5)

}