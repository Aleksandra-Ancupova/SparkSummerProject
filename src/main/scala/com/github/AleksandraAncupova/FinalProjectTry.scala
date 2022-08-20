package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, expr, lit, max, mean, min, round, stddev}

object FinalProjectTry extends App {

  val spark = getSpark("Sparky")
  val filePath = "src/resources/csv/stock_prices_.csv"

  val df = readDataWithView(spark, filePath)
  df.orderBy("date").show(10)

  /// this works fine:
//    df.withColumn("daily_return", expr("round(close - open, 2)"))
//    .groupBy("date")
//    .agg(expr("round(avg(daily_return),2)").alias("Average Return per Stock"),
//      expr("round(sum(daily_return), 2)").alias("Total Return of all Stocks")
//    )
//      .orderBy("date")
//      .show()

  // in percent

  val stockAvgDF = df.withColumn("daily_return", expr("(close - open)/open * 100"))
    .groupBy("date")
      .agg(expr("round(avg(daily_return),2)").alias("Avg Return/Stock, %"),
        expr("round(sum(daily_return), 2)").alias("Total Stock Return, %")
      )
        .orderBy("date")


  stockAvgDF.show()

  // Save the results to the file as Parquet(CSV and SQL are optional)

//  stockAvgDF.write
//    .format("csv")
//    .mode("overwrite")
//    .option("sep", "\t")
//    .option("header", true)
//    .save("src/resources/final/my-csv-file.csv")
//
//  stockAvgDF.write
//    .format("parquet")
//    .mode("overwrite")
//    .save("src/resources/final/my-parquet-file.parquet")

 // Which stock was traded most frequently - as measured by closing price * volume - on average?

  df.withColumn("Frequency", expr("close * volume"))
    .groupBy("ticker")
    .agg(avg("Frequency").alias("Trading Frequency"))
    .orderBy("Trading Frequency")
    .show()

  //Which stock was the most volatile as measured by annualized standard deviation of daily returns?


  df.agg(max("date")).show()
  df.agg(min("date")).show()

  stockAvgDF.agg(stddev("Avg Return/Stock, %")).show()


  //window option
  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy(col("date").asc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val dfWithReturn = df.withColumn("daily_return, %", expr("round((close - open)/open * 100, 2)"))
  dfWithReturn.show()

  val avgDailyReturn = avg(col("daily_return, %")).over(windowSpec)

  val SDForDailyReturn = stddev(col("daily_return, %")).over(windowSpec)

  val dfVolatile = dfWithReturn.select(col("*"),
    round(avgDailyReturn,2).alias("avgDailyReturn, %"),
    round(SDForDailyReturn,2).alias("StDevforDailyReturn"))


  dfVolatile.show()

  dfVolatile.where("date = '2016-11-02'")
    .withColumn("Volatility, %", expr("round(StDevforDailyReturn * sqrt(250), 2)"))
    .show()


}
