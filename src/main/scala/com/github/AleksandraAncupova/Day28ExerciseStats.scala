package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{collect_list, collect_set, corr, covar_pop, kurtosis, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28ExerciseStats extends App {

  // TODO load March 8th of 2011
  // lets see avg, variance, std, skew, kurtosis, correlatiom amd population covariance

  // transform unique countries for that dat into regular Scala Array of strings

  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/by-day/2011-03-08.csv"
  val df = readDataWithView(spark, filePath)

  df.select(mean("UnitPrice"),
    var_pop("UnitPrice"),
    var_samp("UnitPrice"),
    stddev_pop("UnitPrice"),
    stddev_samp("UnitPrice"),
    skewness("UnitPrice"),
    kurtosis("UnitPrice"))
  .show()

  df.select(covar_pop("UnitPrice", "Quantity"),
    corr("UnitPrice", "Quantity"))
    .show()


  df.agg(collect_list("Country"))

  val countries = df.agg(collect_set("Country"))

  countries.show(truncate = false)

  //  val countriesArray = countries.collect().toString.split(",")
  // countriesArray.foreach(println)

  val countriesArray = countries.collect().mkString(",")

  println(countriesArray)

}
