package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, datediff, months_between, round}

object Day25Exercise extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)

  //TODO add new column with current date, timestamp, day diff and month diff

  df.select("InvoiceDate")
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
    .withColumn("dayDiff", datediff(col("now"), col("InvoiceDate")))
    .withColumn("monthlyDiff", round(months_between(col("now"), col("InvoiceDate"))))
    .show(10, false)

}
