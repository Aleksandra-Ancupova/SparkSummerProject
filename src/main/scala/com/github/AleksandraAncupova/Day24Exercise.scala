package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, expr, initcap, lit, lpad, regexp_replace, rpad}

object Day24Exercise extends App {

  // TODO open up March 1st of 2011, CSV

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.readDataWithView(spark, filePath)

  //Select Capitalized Description Column
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //ideally there would be even number of _______LATVIA__________ (30 total)
  //select Description column again with all occurrences of metal or wood replaced with material
  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns


//  df.select(
//    col("Description"),
//    col("Country"),
//    rpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("Country_"),
//    lpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("_Country_")
//  ).show(10,false)

  //lpad(rpad(col("Country"), 15 + col("Country").toString.length/2, "_"), 30, "_").as("__Country__")

  df.select(
    col("Description"),
    col("Country"),
    rpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("Country_"),
    lpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("_Country_"),
    lpad(rpad(col("Country"), 15 + col("Country").toString.length/2, "_"), 30, "_").as("__Country__")
  ).show(80,false)

    // universal
  df.select(
    col("Description"),
    col("Country"),
    expr("lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___")
  ).show(100,false)



  spark.sql(
    """
      |SELECT Description,
      |Country,
      |rpad(Country, 22, '_'),
      |lpad(Country, 22, '_'),
      |lpad(rpad(Country, int(15+(CHAR_LENGTH(Country)/2)), '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin
  )
    .sample(false, fraction = 0.3)
    .show(80, false)



  val materials = Seq("wood", "metal", "ivory")
  val regexString = materials.map(_.toUpperCase).mkString("|")
  println(regexString)


  df.select(
    regexp_replace(col("Description"), regexString, "material").alias("Material_Desc"),
    col("Description"))
    .show(10,false)





}
