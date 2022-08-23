package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, expr, max, min, round, stddev}

object FinalProjectPart1 extends App {

  val spark = getSpark("Sparky")
  val filePath = "src/resources/csv/stock_prices_.csv"

  //Load up stock_prices.csv as a DataFrame
  val df = readDataWithView(spark, filePath)

  // Compute the average daily return of every stock for every date
  val dfWithReturn = df.withColumn("dailyReturn", expr("round((close - open)/open * 100, 2)"))

  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy(col("date").asc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val avgDailyReturn = avg(col("dailyReturn")).over(windowSpec)

  val dfWithAvgReturn= dfWithReturn.select(col("*"),
    round(avgDailyReturn,2).alias("avgDailyReturn"))

  dfWithAvgReturn.show()

  // Save the results to the file as Parquet(CSV and SQL are optional)
  dfWithAvgReturn.write
     .format("parquet")
     .mode("overwrite")
     .save("src/resources/final/my-parquet-file.parquet")

  dfWithAvgReturn.write
      .format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .option("header", true)
      .save("src/resources/final/my-csv-file.csv")

  //Which stock was traded most frequently - as measured by closing price * volume - on average?

  df.withColumn("frequency", expr("close * volume"))
    .groupBy("ticker")
    .agg(avg("frequency").alias("avgTradingFrequency"))
    .orderBy(desc("avgTradingFrequency"))
    .show()

  println("Apple Stock was the most frequently traded throughout given period")

  //Which stock was the most volatile as measured by annualized standard deviation of daily returns?

  df.agg(max("date")).show()
  df.agg(min("date")).show()

  val SDForDailyReturn = stddev(col("dailyReturn")).over(windowSpec)

  val dfVolatile = dfWithAvgReturn.select(col("*"),
    round(SDForDailyReturn,2).alias("stDevOfDailyReturn"))

  dfVolatile.where("date = '2016-11-02'")
    .withColumn("volatility", expr("round(stDevOfDailyReturn * sqrt(250), 2)"))
    .orderBy(desc("volatility"))
    .show()

  println("Tesla Stock was the most volatile throughout given period: 35.26% volatility")


}
