package spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.beans.BeanInfo

@BeanInfo
case class LabeledDocument(id: Long, text: String, label: Double)

@BeanInfo
case class Document(id: Long, text: String)

object SimpleTextClassificationPipeline {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SimpleTextClassificationPipeline")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import sqlContext.implicits._

    // Prepare training documents, which are labeled.
    val training = sc.parallelize(Seq(
      LabeledDocument(0L, "a b c d e ", 1.0),
      LabeledDocument(1L, "spark hadoop", 0.0),
      LabeledDocument(1L, "apache", 0.0),
      LabeledDocument(1L, "kafka", 0.0),
      LabeledDocument(1L, "mapreduce in hdi", 0.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "f g h", 1.0),
      LabeledDocument(3L, "f3 k gf fe", 1.0)))

    training.toDF().show()

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training.toDF())
    model.write.overwrite().save("tmp/steaming/logisticRegression")

    // Prepare test documents, which are unlabeled.
    val test = sc.parallelize(Seq(
      Document(4L, "spark i j k"),
      Document(5L, "l m n"),
      Document(6L, "spark hadoop spark"),
      Document(7L, "apache hadoop")))

    // Make predictions on test documents.
    model.transform(test.toDF())
      .select("id", "text", "probability", "prediction").show()


    val lModel = PipelineModel.load("tmp/steaming/logisticRegression")
    sc.stop()
  }
}
