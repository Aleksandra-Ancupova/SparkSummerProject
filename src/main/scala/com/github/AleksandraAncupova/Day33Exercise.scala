package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.Day31ReadingData.spark
import com.github.AleksandraAncupova.Day33PreprocessingData.sales
import com.github.AleksandraAncupova.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.expr

import scala.io.Source.fromURL

object Day33Exercise extends App {
  val spark = getSpark("Sparky")

  //TODO Read text from url
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark
  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well

  val path = "src/resources/alice_book.txt"


  //alternative download and read from file locally
  //TODO create a DataFrame with a single column called text which contains above book line by line

  val dfFromText = spark.read
    .textFile(path)


  dfFromText.show(10, false)

  val df = dfFromText.withColumnRenamed("value", "text")

  //TODO create new column called words with will contain Tokenized words of text column

  val tkn = new Tokenizer().setInputCol("text")
  val tokenDF = tkn.transform(df.select("text"))

  val df2 = tokenDF.withColumnRenamed("tok_f0a4751a0381__output", "words") // doesn't work
  df2.show(false)

  //TODO create column called textLen which will be a character count in text column
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark
  tokenDF
 df2.withColumn("textLen", expr("CHAR_LENGTH 'text'")).show()

  //TODO create column wordCount which will be a count of words in words column
  //can use count or length - words column will be Array type

  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features
  //features column will have a Vector with two of those values

  //TODO create StandardScaler which will take features column and output column called scaledFeatures
  //it should be using mean and variance (so both true)

  //TODO create a dataframe with all these columns - save to alice.csv file with all columns

}
