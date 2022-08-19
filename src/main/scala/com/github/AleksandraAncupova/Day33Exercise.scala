package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.ml.feature.{StandardScaler, Tokenizer, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat_ws, expr, size}

object Day33Exercise extends App {
  val spark = getSpark("Sparky")

  //TODO Read text from url
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark
  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well
  //alternative download and read from file locally
  //TODO create a DataFrame with a single column called text which contains above book line by line

  val path = "src/resources/alice_book.txt"

  val df = spark.read
    .textFile(path)
    .withColumnRenamed("value", "text")

  df.show(10, false)

  //TODO create new column called words with will contain Tokenized words of text column

  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val tokenDF = tkn.transform(df.select("text"))

  tokenDF.printSchema()

  //TODO create column called textLen which will be a character count in text column
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark
  tokenDF

  //TODO create column wordCount which will be a count of words in words column
  //can use count or length - words column will be Array type

  val df2 = tokenDF.withColumn("textLen", expr("CHAR_LENGTH(text)"))
    .withColumn("wordCount", size(col("words")))

  df2.show(5, false)

  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features
  //features column will have a Vector with two of those values

  val va = new VectorAssembler()
    .setInputCols(Array("textLen", "wordCount"))
    .setOutputCol("features")

  val dfAssembled = va.transform(df2)

  //TODO create StandardScaler which will take features column and output column called scaledFeatures
  //it should be using mean and variance (so both true)

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true )

  val finalDF = scaler.fit(dfAssembled).transform(dfAssembled)

  finalDF.printSchema()
  finalDF.show(10, false)


  //TODO create a dataframe with all these columns - save to alice.csv file with all columns

  // concat works!!!
//  val finalDFString = finalDF.withColumn("words", concat_ws(",", col("Words")))
//
//  finalDFString.select("text", "words", "textLen", "wordCount", "features", "scaledFeatures" ).show(10)
//  finalDFString.printSchema()

  // casting is neater

  val dfCSV = finalDF
    .withColumn("Words", col("words").cast("string"))
    .withColumn("Features", col("features").cast("string"))
    .withColumn("ScaledFeatures", col("scaledFeatures").cast("string"))
    .select("text", "Words", "textLen", "wordCount", "Features", "ScaledFeatures")

    dfCSV.show()

    dfCSV.write
    .format("csv")
    .mode("overwrite")
    .option("header", true)
    .option("sep", "\t")
    .save("src/resources/alice/alice.csv")



}
