package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}

object Day30Exercise extends App {

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL

  val spark = getSpark("Sparky")
  import spark.implicits._

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePathCust = "src/resources/retail-data/customers.csv"

  val retailData = readDataWithView(spark, filePath)
  retailData.createOrReplaceTempView("retailData")

  val custData = readDataWithView(spark, filePathCust)
  custData.createOrReplaceTempView("custData")

  val joinExpression = retailData.col("CustomerID") === custData.col("id")

  retailData.join(custData, joinExpression).show()


  spark.sql(
    """
      |SELECT * FROM retailData JOIN custData
      |ON retailData.CustomerID = custData.id
      |""".stripMargin)
    .show()

}
