package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{approx_count_distinct, count, countDistinct, isnull}

object Day27Aggregations extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val df = readDataWithView(spark, filePath)


  df.show(5, false)
  println(df.count(), "rows")

  df.select(count("StockCode")).show()

  df.select(countDistinct("StockCode")).show()

  // dafault RSD of 0.05 :
  df.select(approx_count_distinct("StockCode")).show()
  df.select(approx_count_distinct("StockCode", 0.01)).show()

  //TODO find count, distinct count and also approximate distinct count for invoiceNo, customerId and unitPrice

  df.select(count("InvoiceNo"),
    count("CustomerID"),
    count("UnitPrice"))
    .show()

  df.select(countDistinct("InvoiceNo"),
    countDistinct("CustomerID"),
    countDistinct("UnitPrice"))
    .show()

  df.select(approx_count_distinct("InvoiceNo"),
    approx_count_distinct("CustomerID"),
    approx_count_distinct("UnitPrice"))
    .show()


}

