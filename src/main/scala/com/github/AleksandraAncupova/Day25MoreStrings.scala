package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, expr}

object Day25MoreStrings extends App {

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)


  // we add a new column with boolean and then filter by it
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(5, false)

  // SQl
  spark.sql(
    """
      |SELECT Description FROM dfTable
      |WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
      |""".stripMargin
  )

  val multipleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = multipleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value

  df.select(selectedColumns:_*).show(10, false)  // we unroll our seq of Column into multiple individual arguments

  df.select(selectedColumns.head, selectedColumns(3), selectedColumns.last, col("Description"))
    .show(5, false)

  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)





}
