package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, myRound, readDataWithView}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

object FinalProjectPart2Model extends App {

  for ((arg, i) <- args.zipWithIndex) {
    println(s" argument No. $i, argument: $arg")
  }
  if (args.length >= 1) {
    println()
  }

  val filePath = "src/resources/csv/stock_prices_.csv"
  val src = if (args.length >= 1) args(0) else filePath
  println(s"My Source file will be $src")

  val spark = getSpark("Sparky")

  val df = readDataWithView(spark, src)

  // creating an array of distinct stocks/tickers

  val stocks = spark.sql(
    """
      |SELECT DISTINCT(ticker) FROM dfTable
      |""".stripMargin)

  val rows = stocks.collect()
  val strings = rows.map(_.getString(0))
  println(strings.mkString(","))

  /**
   * main loop that creates and assesses model for each stock
   */
  def createAndAssessModel(): Unit = {
  for (st <- strings) {
    //preprocessing data
    val myDF = preprocessing(df, st)

    //building a model, predicting
    val rFormula = new RFormula()
      .setFormula("nextDayClose ~ open + high + low + close")

    val newDF = rFormula.fit(myDF).transform(myDF)

    val Array(train, test) = newDF.randomSplit(Array(0.8, 0.2))

    val linReg = new LinearRegression()
    val lrModel = linReg.fit(train)

    val predictDF = lrModel.transform(test)
    predictDF.show()

    //evaluating the model
    testLinearRegression(lrModel)
  }

  }

  /**
   * creates a new column for a dataframe with next day close price
   * @param df dataframe to process
   * @param stockName stock name
   * @return a new dataframe with extra column
   */
  def preprocessing(df: DataFrame, stockName: String): DataFrame = {
    val stock = df.where(col("ticker") === stockName)

    val windowSpec = Window.partitionBy("ticker").orderBy("date")
    val nextDayClose = lead(col("close"), 1).over(windowSpec)

    val stockDF = stock.withColumn("nextDayClose", nextDayClose)

    val stockDFNoNull = stockDF.na.drop
    stockDFNoNull
  }

  /**
   * prints statistics (intercept, coefficients, MAE, MSE) for a Linear Regression Model
   * @param model Linear Regression Model
   */
  def testLinearRegression(model: LinearRegressionModel): Unit = {
    val intercept = myRound(model.intercept,2)
    val coefficient = model.coefficients
    val c1 = myRound(coefficient(0), 2)
    val c2 = myRound(coefficient(1), 2)
    val c3 = myRound(coefficient(2), 2)
    val c4 = myRound(coefficient(3), 2)
    val mae = myRound(model.summary.meanAbsoluteError,2)
    val mse = myRound(model.summary.meanSquaredError,2)
    println(s"The model has following intercept: $intercept; coefficients: $c1, $c2, $c3, $c4; MAE: $mae; MSE: $mse")
  }

  createAndAssessModel()

}
