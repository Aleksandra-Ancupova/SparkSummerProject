package com.github.AleksandraAncupova

import org.apache.spark.sql.functions.desc

object Day23Exercise extends App {

  // TODO load data from 1st of March 2011 into DF
  // get all purchases that have been made from Finland
  // sort by unit price, limit purchases to 20
  // collect results to an array of rows, print all rows

  val spark = SparkUtil.getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  val sortedFinland = df.where("Country = 'Finland'")
    .orderBy(desc("UnitPrice"))
    .limit(20)
    .collect()

  sortedFinland.foreach(println)
  println(s"Here is how many rows were collected: ${sortedFinland.length}")



}
