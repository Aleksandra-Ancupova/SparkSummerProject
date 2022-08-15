package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.Day31ReadingData.spark
import com.github.AleksandraAncupova.SparkUtil.getSpark

object Day31Exercise extends App {

  //TODO read parquet file from src/resources/regression
  //TODO print schema
  //TODO print a sample of some rows
  //TODO show some basic statistics - describe would be a good start
  //TODO if you encounter warning reading data THEN save into src/resources/regression_fixed

  val spark = getSpark("Sparky")

  val regDF = spark.read.format("parquet")
    .load("src/resources/regression")

  regDF.show(10)
  regDF.describe().show()
  regDF.printSchema()
}
