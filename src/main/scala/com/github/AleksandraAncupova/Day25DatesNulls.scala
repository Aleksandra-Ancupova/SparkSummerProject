package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, col, expr, lit, to_date, to_timestamp}

object Day25DatesNulls extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)

  // returns null for 2016-20-12 because of the bad format
  df.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(2)

  //
  val dateFormat = "yyyy-dd-MM"
  val euroFormat = "dd-MM-yy"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
    to_date(lit("02-08-22"), euroFormat).alias("date3"))

  cleanDateDF.createOrReplaceTempView("dateTable2")
  cleanDateDF.show(3, false)

//  spark.sql(
//    """
//      |SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
//      |FROM dfTable""".stripMargin
//  )

  //
  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  cleanDateDF.filter(col("date3") > lit("2021-12-12")).show()

  //Working with Nulls in Data

  df.withColumn("mynulls", expr("null"))
    .select(coalesce(col("Description"), col("CustomerId")))
    .show()

  spark.sql(
    """
      |SELECT
      |ifnull(null, 'return_value'),
      |nullif('value', 'value'),
      |nvl(null, 'return_value'),
      |nvl2('not_null', 'return_value', "else_value")
      |FROM dateTable2 LIMIT 1
      |""".stripMargin)
    .show(2)

  println(s"Originally df is size ${df.count()}")

  println(df.na.drop().count())
  println(df.na.drop("any").count) //same as above drops rows where any column is null

  //all will drop rows only if ALL columns are null
  println(df.na.drop("all").count())

  //We can also apply this to certain sets of columns by passing in an array of columns:
  println("After dropping empty StockCode AND invoiceNo")
  println(df.na.drop("all", Seq("StockCode", "InvoiceNo")).count())
  println("After dropping when null Description")
  println(df.na.drop("all", Seq("Description")).count())


}
