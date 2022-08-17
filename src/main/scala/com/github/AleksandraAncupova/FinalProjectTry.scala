package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{expr, lit, mean, round}

object FinalProjectTry extends App {

  val spark = getSpark("Sparky")
  val filePath = "src/resources/csv/stock_prices_.csv"

  val df = readDataWithView(spark, filePath)
  df.orderBy("date").show(10)

  /// this works fine:
    df.withColumn("daily_return", expr("round(close - open, 2)"))
    .groupBy("date")
    .agg(expr("round(avg(daily_return),2)").alias("Average Return per Stock"),
      expr("round(sum(daily_return), 2)").alias("Total Return of all Stocks")
    )
      .orderBy("date")
      .show()

  // in percent

  val stockAvgDF = df.withColumn("daily_return", expr("(close - open)/open * 100"))
    .groupBy("date")
      .agg(expr("round(avg(daily_return),2)").alias("Avg Return/Stock, %"),
        expr("round(sum(daily_return), 2)").alias("Total Stock Return, %")
      )
        .orderBy("date")


  stockAvgDF.show()

  // writing to a file

  stockAvgDF.write
    .format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .option("header", true)
    .save("src/resources/final/my-csv-file.csv")

  stockAvgDF.write
    .format("parquet")
    .mode("overwrite")
    .save("src/resources/final/my-parquet-file.parquet")

}
