package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_add, date_sub, datediff, lit, months_between, to_date, to_timestamp}

object Day25Dates_TimeStamps extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)

  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")

  dateDF.show(10, false)


  dateDF.select(col("today"),
    date_sub(col("today"), 5),
    date_add(col("today"), 7),
    date_add(col("today"), 365))
    .show(3)

  spark.sql(
    """
      |SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable
      |""".stripMargin
  ).show(3)

  // common task is to look at difference between dates
  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today"))).show(1)

  dateDF.select(
    to_date(lit("2022-06-13")).alias("start"),
    to_date(lit("2022-08-30")).alias("end"))
    .withColumn("monthlyDifference", months_between(col("start"), col("end")))
    .withColumn("dayDifference", datediff(col("end"), col("start")))
    .show(2)


  val dateTimeDF = spark.range(5)
    .withColumn("date", to_date(lit("2022-08-02")))
    .withColumn("timeStamp", to_timestamp(col("date")))  // we can do this since we just made date column
    .withColumn("badDate", to_date(lit("2022-08nota dfd-02")))


    dateTimeDF.show(5)
    dateTimeDF.printSchema()

}
