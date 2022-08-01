package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, initcap, lit, lower, lpad, ltrim, regexp_replace, rpad, rtrim, trim, upper}

object Day24Strings extends App {

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = SparkUtil.readCSVwithView(spark, filePath)

  // working with string
  df.select(col("Description"),
    initcap(col("Description")).alias("DescInitCap")).show(3, false)

  // all SQL
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3,false) // we need to see full info


  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(3)

  spark.sql("SELECT Description, lower(Description), Upper(lower(Description)) FROM dfTable")
    .show(3,false)



  df.select(
    col("CustomerId"),
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp"),
  lpad(rpad(lit("HELLO"), 10, "*"), 5, "*").as("pad5starts")
  ).show(2)

  spark.sql(
    """
      |SELECT
      |CustomerId,
      |ltrim(' HELLLOOOO ') as ltrim,
      |rtrim(' HELLLOOOO '),
      |trim(' HELLLOOOO '),
      |lpad('HELLOOOO ', 3, ' '),
      |rpad('HELLOOOO ', 10, ' ')
      |FROM dfTable
      |""".stripMargin)
    .show(2 )


  // regular expressions

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  println(regexString)  // "BLACK|WHITE|RED|GREEN|BLUE"

  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description"))
    .show(5,false)


  spark.sql(
    """
      |
      |SELECT
      |regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'colorful') as
      |color_clean, Description
      |FROM dfTable
      |""".stripMargin)
    .show(5,false)





}
