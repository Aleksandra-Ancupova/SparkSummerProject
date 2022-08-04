package com.github.AleksandraAncupova

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col

object Day22MoreTransformations extends App {
  val spark = SparkUtil.getSpark("Sparky")

  spark.conf.set("spark.sql.caseSensitive", true)

  val flightPath = "src/resources/flight-data/json/2015-summary.json"
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)
  df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(5)

  val dfWith3Counts = df.withColumn("count2", col("count").cast("int"))
    .withColumn("count3", col("count").cast("double"))

  dfWith3Counts.show(5)
  dfWith3Counts.printSchema()


  df.filter(col("count") < 2).show(2)  // less used
  df.where("count < 2").show(2) //  more common

  df.where("count > 5 AND count < 10").show(5)  // works, but better chain multiple filters

  df.where("count >5")
    .where("count < 10")
    .show(5)


  df.where(col("count") < 2)
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
    .show(3)

  val countUniqueFlights = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  println(s"Number of unique flights $countUniqueFlights")

  // random samples
  val seed = 5
  val withReplacement = false   // if you set it to true that means you'll be putting your row sample back into the jar
  val fraction = 0.1 //so 10%

  val dfSample = df.sample(withReplacement, fraction, seed)

  dfSample.show(5)
  println(s"We got ${dfSample.count()} samples")

  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
  for((dFrame, i) <- dataFrames.zipWithIndex) {
    println(s"DataFrame No. $i has ${dFrame.count} rows")
  }

  def getDataFramaStats(dFrames: Array[Dataset[Row]], df:DataFrame): Array[Long] = {
    dFrames.map(d => d.count() * 100 / df.count())
  }

  val dPercentages = getDataFramaStats(dataFrames, df)
  println("dataFrame percentages:")
  dPercentages.foreach(println)

  val dFrames23split = df.randomSplit(Array(2,3), seed)
  getDataFramaStats(dFrames23split, df).foreach(println)


}
