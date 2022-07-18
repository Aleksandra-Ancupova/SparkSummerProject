package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession

object Day17SparkStart extends App {
  println(s"Testing Scala Version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started in Spark version ${spark.version}")


  val myRange = spark.range(1000).toDF("number")
  val divisibleBy5 = myRange.where("number % 5 = 0")
  divisibleBy5.show(10) // show first rows

  val range100 = spark.range(100).toDF("numbers")
  val divisibleBy10 = range100.where("numbers % 10 = 0")
  divisibleBy10.show()

}