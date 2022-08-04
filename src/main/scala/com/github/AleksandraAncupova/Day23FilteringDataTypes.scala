package com.github.AleksandraAncupova

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, corr, countDistinct, expr, mean, ntile, pow, round}

object Day23FilteringDataTypes extends App {

  val spark = SparkUtil.getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()

  // Working with Booleans
  val priceFilter = col("UnitPrice") > 600
  val descriptionFilter = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter))
    .show()


  val DOTCodeFilter = col("StockCode") === "DOT"
  //so we add a Boolean column which shows whether StockCode is name DOT
  df.withColumn("stockCodeDOT", DOTCodeFilter).show(10)
  df.withColumn("stockCodeDOT", DOTCodeFilter)
    .where("stockCodeDOT")
    .show()

  // here we create new boolean column with 3 filters at onve
  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)

  df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)


  // this is for Null checking
  df.where(col("Description").eqNullSafe("hello")).show()

  // Working with Numbers
  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(expr("CustomerId"),
    col("Quantity"),
    col("UnitPrice"),
    fabricatedQuantity.alias("realQuantity"))
    .show(2)

  // we can do the same thing like this

  df.selectExpr(
    "CustomerId",
    "Quantity as OldQuant",
    "UnitPrice",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
    .show(2)

  // we can use any SQL functions available at Spark

  df.selectExpr(
    "CustomerId",
    "Quantity as OldQuant",
    "UnitPrice",
    "ROUND((POWER((Quantity * UnitPrice), 2.0) + 5), 2) as realQuantity")
    .show(4)

  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

  // correlation functionality, inverse = -1 and positive = 1

  val corrCoefficient =  df.stat.corr("Quantity", "UnitPrice")
  println(s"Pearson correlations coefficient for Quantity and unitPrice is $corrCoefficient")

  df.select(corr("Quantity", "UnitPrice")).show()

  df.describe().show()

  df.select(mean("Quantity"), mean("UnitPrice")).show()

  // median?
  val colName = "UnitPrice"
  val quantileProbs = Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99)
  val relError = 0.05
  val quantilePrices = df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

  for ((prob, price) <- quantileProbs zip quantilePrices) {
    println(s"Quantile $prob - price $price")
  }
  // so the median price is approximately 2.51

  // default is quartile (4 cut points)
  def getQuantiles(df: DataFrame, colName: String, quantileProbs: Array[Double] = Array(0.25, 0.5, 0.75, 0.99), relError: Double = 0.05): Array[Double] = {
    df.stat.approxQuantile(colName, quantileProbs, relError)
  }

  def printQuartiles(df: DataFrame, colName: String, quantileProbs: Array[Double] = Array(0.25, 0.5, 0.75, 0.99), relError: Double = 0.05): Unit = {
    val quantiles = getQuantiles(df, colName, quantileProbs, relError)
    println(s"For column $colName")
    for ((prob, cutPoint) <- quantileProbs zip quantiles) {
      println(s"Quantile $prob - cutPoint $cutPoint")
    }
  }

  val deciles = (1 to 10).map(n => n.toDouble/10).toArray
  printQuartiles(df, "UnitPrice", deciles)

  // df.stat.crosstab("StockCode", "Quantity").show()

  df.groupBy("Country").count().show()

  for (col <- df.columns) {
    val count = df.groupBy(col).count().count()
    println(s"Column $col has $count distinct values")
  }

  // val windowSpec = Window.partitionBy("UnitPrice").orderBy("UnitPrice")
  val windowSpec = Window.partitionBy().orderBy("UnitPrice")

  df.select(col("Description"), col("UnitPrice"),
    ntile(4).over(windowSpec).alias("quantile_rank")).show()


}
