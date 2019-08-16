package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Encoder, SQLContext, SparkSession}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
/*
* link: https://scalac.io/scala-spark-ml-machine-learning-introduction/
* */
object IdentifyWines {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Wine price regression")
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val schemaStruct = StructType(
      StructField("points", DoubleType)::
      StructField("country", StringType)::
      StructField("price", DoubleType)::Nil
    )

    import spark.implicits._

    val df = spark.read
      .option("header", true)
      .csv("data/wine-data.csv")
      .select($"points".cast(DoubleType),
          $"country".cast(StringType),
          $"price".cast(DoubleType)
      )
      .na.drop()

    df.show()
    // split into the set into training and test data
    val Array(trainingData, testData) = df.randomSplit(Array(0.8,0.2))
    val labelColumn = "price"

    val countryIndexer = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("countryIndex")

    // define the assembler to collect the columns into a new column with a single vector - "features"
    val assembler = new VectorAssembler()
      .setInputCols(Array("points","countryIndex"))
      .setOutputCol("features")

    // Gradient-boosted tree estimator
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("predicted "+ labelColumn)
      .setMaxBins(50)
      .setMaxIter(50)

    val stages = Array(
      countryIndexer,
      assembler,
      gbt
    )

    // contruct the pipeline
    val pipeline = new Pipeline().setStages(stages)

    // fit our dataframe into pipeline
    val model = pipeline.fit(trainingData)

    // prediction using model and test data
    var predictions = model.transform(testData)


    val dis :(Double, Double) => Double = _ - _

    val dis2 = udf(dis)


//  predictions =  predictions.withColumn("distamce", dis2($"price", $"predicted price"))
//  val predict = predictions.withColumn("Sub", when(col("price").isNull, lit(0)).otherwise(col("price")) - when(col("predicted price").isNull, lit(0)).otherwise(col("predicted price")))


    predictions =  predictions.withColumn("distance", abs(col("price") - col("predicted price")) )

    predictions.orderBy($"distance".desc).show()

    // evaluation using RMSE
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("predicted "+ labelColumn)
      .setMetricName("rmse")


    val error = evaluator.evaluate(predictions)

    println(error)



  }
}
