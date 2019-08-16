package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object ETSelectFeatures {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Extract, Tranform, select features")
      .master("local[*]")
      .getOrCreate()
    val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

    /*
    * HashingTF*/
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I yêu Việt Nam"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(15)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(false)

    /*
    * Word2Vec*/
    val documentDF = spark.createDataFrame(Seq(
      "Hi I header about Spark and Spark".split(" "),
      "I wish java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    documentDF.show(false)

    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(4)
      .setMinCount(0)
    val model = word2vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach{ case Row(text: Seq[_], features: Vector) =>
        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }


    /*
    * CountVectorizer*/
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    val cvm = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")



    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)



    val ngram = new NGram().setN(2).setInputCol("raw").setOutputCol("ngrams")
    val ngramDF = ngram.transform(dataSet)
    ngramDF.show(false)


  }
}
