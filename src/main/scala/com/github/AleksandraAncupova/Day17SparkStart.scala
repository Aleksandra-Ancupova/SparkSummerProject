package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession

object Day17SparkStart extends App {
  println(s"Testing Scala Version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started in Spark version ${spark.version}")

}
