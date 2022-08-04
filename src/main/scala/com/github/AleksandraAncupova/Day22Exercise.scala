package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.Day22MoreTransformations.{dataFrames, getDataFramaStats}
import org.apache.spark.sql.functions.col

object Day22Exercise extends App {

  val spark = SparkUtil.getSpark("Sparky")
  spark.conf.set("spark.sql.caseSensitive", true)

  val flightPath = "src/resources/flight-data/json/2014-summary.json"
  val df = spark.read.format("json")
    .load(flightPath)

  // TODO filter flights FROM US that happened more than 10 times

  df.where("ORIGIN_COUNTRY_NAME = 'United States'")
    .where("count > 10")
    .show()


  // TODO random sample of 30%

  val seed = 5
  val withReplacement = false
  val fraction = 0.3 //so 30% ?

  val dfSample = df.sample(withReplacement, fraction, seed)

  dfSample.show(5)
  println(s"We got ${dfSample.count()} samples")


  // TODO a split of 2,9,5 for the dataset
  val randomDfs = df.randomSplit(Array(2, 9, 5), seed)
  for((dFrame, i) <- randomDfs.zipWithIndex) {
    println(s"DataFrame No. $i has ${dFrame.count} rows")
  }

  val splitPercentages = getDataFramaStats(randomDfs, df)
  println("Here are the the split in percentages:")
  splitPercentages.foreach(println)

  val unionFirst2 =  randomDfs(0).union( randomDfs(1))
  unionFirst2.show(5)



}