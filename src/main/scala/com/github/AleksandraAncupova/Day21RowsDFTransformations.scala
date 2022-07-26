package com.github.AleksandraAncupova

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Day21RowsDFTransformations extends App {
  val spark = SparkUtil.getSpark("BasicSpark")

   val myRow = Row("Hello Sparky", null, 555, false, 3.1415926)
  println(myRow(0))
  println(myRow(0).asInstanceOf[String])
  println(myRow.getString(0))
  val myGreeting = myRow.getString(0)
  println(myRow.getInt(2))
  val myPi = myRow.getDouble(4)  // so 5th element
  println(myPi)

  // we can print schema for a row, but there is none
  println(myRow.schema)


  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  // so
  val df = spark.read.format("json")
    .load(flightPath)

  df.createOrReplaceTempView("dfTable") // view (virtual Table) needed to make SQL queries

  df.show(5)

  val myManualSchema = new StructType(Array(
    StructField("some", StringType, true),
    StructField("col", StringType, true),
    StructField("names", LongType, false)))

  val myRows = Seq(Row("Hello", null, 1L),
    Row("Sparky", "somestring", 151L),
    Row("Valdies", "my data", 9000L),
    Row(null, "my data", 151L)
  )

  // we need to get down to RDD structure
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, myManualSchema)
  myDf.show()


  df.select("DEST_COUNTRY_NAME").show(2)

  val newDF = df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
  newDF.show(3)

  val sqlWay = spark.sql("""
      |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME
      |FROM dfTable
      |LIMIT 10
      |""".stripMargin)

  sqlWay.show(3)

  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


  // TODO create 3 rows with the following data formats:
  // TODO string - food name, int - quantity, long - price, boolean for is it vegan
  // TODO create a DF called foodFrame which will hold those rows and show them
  // TODO use Select or/an SQL syntax to show only name an qty


  val myManualSchemaFood = new StructType(Array(
    StructField("food_name", StringType, false),
    StructField("qty_kg", IntegerType, false),
    StructField("price_eur", DoubleType, false),
    StructField("is_it_vegan", BooleanType, true)))

  val myFoodRows = Seq(Row("Tomato Salad", 1, 3.5, true),
    Row("Lasagna", 1, 6.5, false),
    Row("Pizza", 1, 9.8, null))

  val myFoodRDD = spark.sparkContext.parallelize(myFoodRows)
  val myFoodDf = spark.createDataFrame(myFoodRDD, myManualSchemaFood)
  myFoodDf.show()

  myFoodDf.select("food_name", "qty_kg").show()

}
