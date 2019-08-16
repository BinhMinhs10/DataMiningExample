package spark_ml
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}



object MLPipelines {

  def main(args: Array[String]): Unit = {
    /* API build on top of Dataframe*/
    /*
    * Transformer = ML model predict
    * Estimator = an algorithm fit or train on data
    * Pipeline = ML workflow
    * */
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Prepare training data from a list of (label, features) tuples.
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    training.show()

    // This instance is an estimator
    val lr = new LogisticRegression()
    println(s"LogisticRegression parameter:\n ${lr.explainParams()}\n")

    lr.setMaxIter(20)
      .setRegParam(0.01)

    val model1 = lr.fit(training)
    println(s"model 1 was fit using parameters: ${model1.parent.extractParamMap()}")

    // cach 2 specify parameters
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // change output column name
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training, paramMapCombined)
    println(s"Model 2 was fit using parameter : ${model2.parent.extractParamMap}")

    // Prepare test data.
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    model2.transform(test)
      .select("features","label","myProbability","prediction")
      .collect()
      .foreach{
        case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          println(s"($features, $label) -> prob = $prob, prediction = $prediction")
      }




  }
}
