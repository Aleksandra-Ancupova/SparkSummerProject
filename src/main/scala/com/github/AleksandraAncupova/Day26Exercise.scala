package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, desc, size, split}

object Day26Exercise extends App {
  //TODO open up 2011-08-04 csv
  //create a new dataframe with all the original columns
  //plus array of of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results


  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"
  val df = SparkUtil.readDataWithView(spark, filePath)

  val newDf = df.withColumn("desc_split", split(col("Description"), " "))
    .withColumn("desc_size", size(col("desc_split")))
    .select("Description",
      "InvoiceNo", "StockCode","Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country",
      "desc_split", "desc_size")
    .where("desc_size >= 3")


  newDf.selectExpr("InvoiceNo", "Description",
    "desc_split[0] as 1st_word",
    "desc_split[1] as 2nd_word",
    "desc_split[2] as 3rd_word")
    .orderBy(desc("1st_word"))
    .show(10,false)

}
