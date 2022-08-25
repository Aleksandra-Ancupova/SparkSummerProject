package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

object Day36Exercise extends App {

  //TODO open "src/resources/csv/range3d"
  val spark = getSpark("Sparky")
  val src = "src/resources/csv/range3d"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(src)

  //TODO Transform x1,x2,x3 into features(you cna use VectorAssembler or RFormula), y can stay, or you can use label/value column
  val rFormula = new RFormula()
    .setFormula("y ~ .")

  val newDF = rFormula.fit(df).transform(df)

  newDF.show()

  //TODO create  a Linear Regression model, fit it to our 3d data
  val linReg = new LinearRegression()

  val lrModel = linReg.fit(newDF)
  val predictDF = lrModel.transform(newDF)

  predictDF.show(10,false)

  //TODO print out intercept
  val intercept = lrModel.intercept
  println(s"Intercept is $intercept")

  //TODO print out all 3 coefficients
  val coefficients = lrModel.coefficients
  println(s"Coefficients are $coefficients")

  //TODO make a prediction if values or x1, x2 and x3 are respectively 100, 50, 1000
  println(lrModel.predict(Vectors.dense(100, 50, 1000)))

}
