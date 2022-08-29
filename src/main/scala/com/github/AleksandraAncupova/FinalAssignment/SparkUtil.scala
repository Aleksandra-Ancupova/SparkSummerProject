package com.github.AleksandraAncupova.FinalAssignment

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtil {

  /**
   * Returns a new or existing Spark session
   *
   * @param appName        - name of our Spark session
   * @param partitionCount default 5
   * @param master         dafault "local" - master URL to connect to
   * @param verbose        - prints debug info
   * @return sparkSession
   */

  def getSpark(appName: String, partitionCount: Int = 1, master: String = "local", verbose: Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose) println(s"Session started on Spark version ${sparkSession.version} with $partitionCount partitions")
    sparkSession
  }

  /**
   *
   * @param spark SparkSession
   * @param filePath File to load
   * @param source File type
   * @param viewName The name of created DataFrame
   * @param header
   * @param inferSchema
   * @param printSchema
   * @param cacheOn
   * @return a DataFrame created with source data
   */

  def readDataWithView(spark: SparkSession,
                       filePath: String,
                       source: String = "csv",
                       viewName: String = "dfTable",
                       header: Boolean = true,
                       inferSchema: Boolean = true,
                       printSchema: Boolean = true,
                       cacheOn: Boolean = true): DataFrame = {
    val df = spark.read.format(source)
      .option("header", header.toString)
      .option("inferSchema", inferSchema.toString)
      .load(filePath)
    if (!viewName.isEmpty) {
      df.createOrReplaceTempView(viewName)
      println(s"Created Temporary View for SQL queries called: $viewName")
    }
    if (printSchema) df.printSchema()
    if (cacheOn) df.cache()
    df
  }


  /**
   *
   * @param n double to round
   * @param precision how many digits after coma to leave
   * @return a rounded double
   */
  def myRound(n: Double, precision: Int = 0): Double = {
    val multiplier = Math.pow(10, precision) //so precision 0 will give us 10 to 0 which is 1
    //    n.round //only 0 precision
    (n * multiplier).round / multiplier //we utilize the built in round
  }

}
