package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, max, rank, to_date, min, desc}

object Day29WindowFunctions extends App {
  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/all/*.csv"

  val df = readDataWithView(spark, filePath)

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"))

  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = rank().over(windowSpec)

  dfWithDate.where("CustomerId IS NOT NULL")
    .orderBy("date", "CustomerId")
    .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity"))
    .show(20, false)


  //TODO create WindowSpec which partitions by StockCode and date, ordered by Price
  //with rows unbounded preceding and current row
  //create max, min, dense rank and rank for the price oer the newly created WindowSpec

  // show top 40 results ordered un descending order by StockCode ad Price
  // show max, min, dense rank and rank for every row as well using new column

  val windowSpec2 = Window
    .partitionBy("StockCode", "date")
    .orderBy(col("UnitPrice").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val maxPrice = max(col("UnitPrice")).over(windowSpec2)
  val minPrice = min(col("UnitPrice")).over(windowSpec2)

  val priceDenseRank = dense_rank().over(windowSpec2)
  val priceRank = rank().over(windowSpec2)

  dfWithDate.where("StockCode IS NOT NULL")
    .orderBy(desc("StockCode"), desc("UnitPrice"))
    .select(
      col("date"),
      col("StockCode"),
      col("UnitPrice"),
      priceRank.alias("priceRank"),
      priceDenseRank.alias("priceDenseRank"),
      maxPrice.alias("maxPrice"),
      minPrice.alias("minPrice"))
    .show(40, false)




}