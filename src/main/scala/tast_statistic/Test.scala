package tast_statistic


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.io.Source



object Test {


  def removeRegex(txt: String, flag: String): String = {
    var RegexList = Map[String, String]()
    RegexList += ("punctuation" -> "[^a-zA-Z0-9]")
    RegexList += ("digits" -> "\\b\\d+\\b")
    RegexList += ("white_space" -> "\\s+")
    RegexList += ("small_words" -> "\\b[a-zA-Z0-9]{1,2}\\b")
    RegexList += ("urls" -> "(https?\\://)\\S+")

    val regex = RegexList.get(flag)
    var cleaned = txt
    regex match {
      case Some(value) =>
        if(value.equals("white_space")) cleaned=txt.replaceAll(value, "")
        else cleaned = txt.replaceAll(value, "")
      case None => println("No regex flag matched")
    }

    cleaned
  }

  def removeCustomWords(txt: String, flag: String): String = {
    var Stopwords = Map[String, List[String]]()
    Stopwords += ("english" -> Source.fromFile("stopwords.txt").getLines().toList)

    var words = txt.split(" ")
    val stopwords = Stopwords.get(flag)
    stopwords match{
      case Some(value) => words = words.filter(x => !value.contains(x) )
      case None => println("No stopword flag matched")
    }
    words.mkString(" ")
  }

  def cleanDocument(document_text: String): String = {
    var text = document_text.toLowerCase
    text = removeRegex(text, "urls")
    text = removeRegex(text, "punctuation")
    text = removeRegex(text, "digits")
    text = removeRegex(text, "small_words")
    text = removeRegex(text, "white_space")
    text = removeCustomWords(text, "english")
    text
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("DC")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    val input_path = "data.txt"
    val input_RDD = sc.textFile(input_path).map(x => {
      val row = x.split(",")
      (cleanDocument(row(1)), row(2))
    })

//    convert RDD to DataFrame
    val trainingDF = sqlContext.createDataFrame(input_RDD)
      .toDF("id", "cleaned", "category")

    val Array(trainingData, testData) = trainingDF.randomSplit(Array(0.7, 0.3))

    // print the training data
    trainingData.show()

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")

    val tokenizer = new Tokenizer()
      .setInputCol("cleaned")
      .setOutputCol("tokens")


    val hashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(20)

    val lr = new LogisticRegression().setMaxIter(100).setRegParam(0.001)
    val ovr = new OneVsRest().setClassifier(lr)

    val pipeline = new Pipeline().setStages(Array(indexer, tokenizer, hashingTF, ovr))
    val model = pipeline.fit(trainingData)

    // Create the classification pipeline and train the model
    val prediction = model.transform(testData).select("id","cleaned_text","category","prediction")

    // Print the predictions
    prediction.foreach(row => println)


  }
}
