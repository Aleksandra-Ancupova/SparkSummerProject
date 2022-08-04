package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array_contains, col, explode, split, struct, map}

object Day26ComplexDataTypes extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)


  // Structs are like Df within DF
  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")

  complexDF.show(5, false)

  complexDF.select("complex.Description").show(3)
  complexDF.select(col("complex").getField("Description")).show(3)
  spark.sql(
    """
      |SELECT complex.Description FROM complexDF
      |""".stripMargin)
    .show(3)

  complexDF.select("complex.*").show(3)

  df.select(functions.split(col("Description"), " ")).show(3, false)


  df.select(functions.split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[0]", "array_col[1]").show(3)

// array length
  df.select(functions.split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", functions.size(col("array_col")))
    .show(5, false)

  //contains checks
  df.select(functions.split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", functions.size(col("array_col")))
    .withColumn("white_exists", array_contains(col("array_col"), "WHITE"))
    .show(5, false)


  //explode
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "splitted", "exploded").show(25, false)

  //Maps
  //Maps are created by using the map function and key-value pairs of columns. You then
  //can select them just like you might select from an array

  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .show(5, false)

//  spark.sql(
//    """
//      |SELECT map(Description, InvoiceNo) as complex_map FROM dfTable
//      |WHERE Description IS NOT NULL
//      |""".stripMargin)
//    .show(5, false)


  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("complex_map['WHITE METAL LANTERN']").show(5, false)

  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("explode(complex_map)").show(12, false)


  //TODO open up


}
