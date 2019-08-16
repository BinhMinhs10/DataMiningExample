package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}


object PipelineExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mepreduce", 0.0)
    )).toDF("id","text","label")
    training.show()

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    // Feature extractor
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")


    val lr = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))


    val model = pipeline.fit(training)

    // save the fitted pipeline to disk
    model.write.overwrite().save("tmp/spark-logistic-regression-model")

    // Save this unfit pipeline to disk
    pipeline.write.overwrite().save("tmp/unfit-lr-model")

    // Load
    val sameModel = PipelineModel.load("tmp/spark-logistic-regression-model")
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach{
        case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob = $prob, prediction=$prediction")
      }
  }
}
