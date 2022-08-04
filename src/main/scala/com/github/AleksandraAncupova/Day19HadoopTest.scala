package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession

object Day19HadoopTest extends App {

  println(s"Testing Hadoop with Scala : ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

}
