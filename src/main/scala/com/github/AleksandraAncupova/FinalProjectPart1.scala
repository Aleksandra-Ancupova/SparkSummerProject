package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, countDistinct, desc, expr, lit, max, min, round, sum, stddev}

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
  val cumReturn = sum(col("dailyReturn")).over(windowSpec)

  val dfWithAvgReturn= dfWithReturn.select(col("*"),
    round(avgDailyReturn,2).alias("avgDailyReturn"),
    round(cumReturn,2).alias("totalReturn"))

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

  // this option shows average return of 5 stocks each day and total return of 5 stocks each day
//      df.withColumn("daily_return", expr("round((close - open)/open * 100, 2)"))
//      .groupBy("date")
//      .agg(expr("round(avg(daily_return),2)").alias("Average Return per Stock"),
//        expr("round(sum(daily_return), 2)").alias("Total Return of all Stocks")
//      )
//        .orderBy("date")
//        .show()

  //Which stock was traded most frequently - as measured by closing price * volume - on average?

  val tradeFrequency = df.withColumn("frequency", expr("close * volume"))
    .groupBy("ticker")
    .agg(avg("frequency").alias("avgTradingFrequency"))
    .orderBy(desc("avgTradingFrequency"))

  tradeFrequency.show()

  val mostTraded = tradeFrequency.collect().map(_.getString(0))
  println(s"${mostTraded.head} stock was the most frequently traded throughout given period")


  //Which stock was the most volatile as measured by annualized standard deviation of daily returns?

  df.agg(max("date")).show()
  df.agg(min("date")).show()

  val SDForDailyReturn = stddev(col("dailyReturn")).over(windowSpec)

  val dfVolatile = dfWithReturn.select(col("*"),
    round(SDForDailyReturn,2).alias("stDevOfDailyReturn"))

  //counting for many periods/date to calculate volatility
  val dateCount = df.select(countDistinct("date")).collect().map(_.toSeq)
  val dateCountInt = dateCount.head.head
  println(dateCountInt)

  val testDF = dfVolatile.withColumn("dateCount", lit(dateCountInt))
  testDF.show()

  val dfYearlyVolatility = testDF.where("date = '2016-11-02'")
    .withColumn("volatility", expr("round(stDevOfDailyReturn * sqrt(dateCount), 2)"))
    .orderBy(desc("volatility"))

  dfYearlyVolatility.show()

  val volatilityArray = dfYearlyVolatility.collect().map(_.toSeq)
  val highestVolatility = volatilityArray.head

  println(s"${highestVolatility(6)} Stock was the most volatile throughout given period: ${highestVolatility.last}")


}
