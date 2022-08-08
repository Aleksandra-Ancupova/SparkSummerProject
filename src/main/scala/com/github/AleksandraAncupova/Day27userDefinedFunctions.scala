package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, round, udf}

object Day27userDefinedFunctions extends App {

  val spark = getSpark("Sparky")
  val df = spark.range(10).toDF("num")
  df.printSchema()
  df.show()

  def power3(n: Double): Double = n*n*n
  def power3int(n: Long): Long = n*n*n
  println(power3(10))  // just a regular function so far

  val power3udf = udf(power3(_:Double):Double)
  val power3intUdf = udf(power3int(_:Long):Long)

  df.withColumn("numCubed", power3udf(col("num")))
    .withColumn("numCubedInt", power3intUdf(col("num")))
    .show()

  // this registers function for Spark SQL
  spark.udf.register("power3", power3(_:Double):Double)

  df.selectExpr("power3(num)").show(5)

  spark.udf.register("power3int", power3int(_:Long):Long)

  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |power3(num),
      |power3int(num)
      |FROM dfTable
      |""".stripMargin)
    .show()

  // TODO create a UDF which converts Fahrenheit to Celsius
  // create DF with temperatureF, -40 to 120
  // register UDF
  // create column temperatureC with conversions

  // show both columns with F temperature at 90 and ending at 110

  // you probably want Double incoming and Double as a return

  val tempDF = spark.range(-40, 120).toDF("temperatureF")

  tempDF.show(10)

  def tempConversion(n: Double): Double = (n - 32) * 5/9

  val tempUdf = udf(tempConversion(_:Double):Double)

  tempDF.withColumn("temperatureC", round(tempUdf(col("temperatureF"))))
    .where("temperatureF BETWEEN 90 AND 110")
    .show(50)



}
