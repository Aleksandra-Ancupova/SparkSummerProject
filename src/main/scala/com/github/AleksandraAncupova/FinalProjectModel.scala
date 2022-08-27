package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lead}

object FinalProjectModel extends App {

  val spark = getSpark("Sparky")
  val filePath = "src/resources/csv/stock_prices_.csv"

  val df = readDataWithView(spark, filePath)

 // val stockArray = Array("AAPL", "BLK")
 // creating an array of distinct stocks/tickers
  val stocks = spark.sql(
    """
      |SELECT DISTINCT(ticker) FROM dfTable
      |""".stripMargin)

  val rows = stocks.collect()
  val strings = rows.map(_.getString(0))
  println(strings.mkString(","))

  // taking each stock and creating a model
  for (st <- strings) {
    val stock = df.where(col("ticker") === st)

    val windowSpec = Window.partitionBy("ticker").orderBy("date")
    val nextDayClose = lead(col("close"), 1).over(windowSpec)

    val stockDF = stock.withColumn("nextDayClose", nextDayClose)

    val stockDFNoNull = stockDF.na.drop

    //stockDFNoNull.orderBy(desc("date")).show()

    val rFormula = new RFormula()
      .setFormula("nextDayClose ~ open + high + low + close + volume")

    val newDF = rFormula.fit(stockDFNoNull).transform(stockDFNoNull)
    //newDF.show(false)

    val linReg = new LinearRegression()
    val lrModel = linReg.fit(newDF)

    val predictDF = lrModel.transform(newDF)
    predictDF.show()

//    evaluation, need function
    println(lrModel.summary.meanAbsoluteError)
    println(lrModel.coefficients)

  }


}
