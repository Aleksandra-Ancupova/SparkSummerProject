package com.github.AleksandraAncupova

import com.github.AleksandraAncupova.SparkUtil.getSpark
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.DataFrame

object Day35Irises extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/irises/iris.data"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  val flowerDF = df.withColumnRenamed("_c4", "irisType")
  flowerDF.show()

  val myRFormula = new RFormula().setFormula("irisType ~ . ")

  val fittedRF = myRFormula.fit(flowerDF)

  val preparedDF = fittedRF.transform(flowerDF)
  preparedDF.show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.75, 0.25))

  //lets check Random Forest
  val rfc = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedModel = rfc.fit(train)

  val testDF = fittedModel.transform(test)

  testDF.show(50, false)

  def showAccuracy(df: DataFrame): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${1.0 - accuracy}")
  }

  showAccuracy(testDF)

}
