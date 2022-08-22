package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.Day33Exercise.{df, spark}
import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}

object Day34Exercise extends App {

  //TODO using tokenized alice - from weekend exercise
  //TODO remove english stopwords

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents(here that means rows)
  //the terms have to occur at least 1 time in each row

  //TODO show first 30 rows of data

  val spark = getSpark("Sparky")

  val path = "src/resources/alice_book.txt"

  val df = spark.read
    .textFile(path)
    .withColumnRenamed("value", "text")

  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val tokenDF = tkn.transform(df.select("text"))

   tokenDF.show()

  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")

  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords) //what we are going to remove
    .setInputCol("words")
    .setOutputCol("noStopWords")

  val noStopWordsDF = stops.transform(tokenDF)

  noStopWordsDF.show(false)

  val cv = new CountVectorizer()
    .setInputCol("noStopWords")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(1) //so term has to appear at least once
    .setMinDF(3) //in how namy docs (rows) word appears

  val fittedDF = cv.fit(noStopWordsDF)

  fittedDF.transform(noStopWordsDF).show(false)

  //we can print out vocabulary , so here I print words index 30 to 49
  println(fittedDF.vocabulary.take(30).mkString("|"))

}
