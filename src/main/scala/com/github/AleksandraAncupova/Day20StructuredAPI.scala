package com.github.AleksandraAncupova

import org.apache.spark.sql.SparkSession

object Day20StructuredAPI extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  val df = spark.range(20).toDF("number")
  df.select(df.col("number") + 100)
    .show(15)

  val tinyRange = spark.range(2).toDF().collect()  // now an Array because of collect

  val arrRow = spark.range(10).toDF(colNames = "myNumber").collect()

   arrRow.take(3).foreach(println)
   arrRow.slice(2,7).foreach(println)
  println("Tail:")
  println(arrRow.last) //should be the row holding 9 here

  // TODO create July numbers 1-31, print numbers, show them

  val julyNumbers = spark.range(1,32).toDF("julyNumbers").collect()
  println("Here are July numbers:")
  julyNumbers.foreach(println)


  // TODO df 100-3100, show last 5 numbers

  val someNumbers = spark.range(100,3101).toDF("someNumbers").collect()
  println("Here are some other numbers:")
  someNumbers.takeRight(5).foreach(println)

}
