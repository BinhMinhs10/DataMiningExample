//package spark_stream
//
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.classification.LogisticRegressionModel
//import org.apache.spark.ml.feature.{CountVectorizerModel, NGram, VectorAssembler}
//import org.apache.spark.ml.linalg.Vector
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import redis.clients.jedis.Jedis
//
//import scala.collection.mutable.ArrayBuffer
//
//object StreamingExample {
//  def cleanText(input: String): String = {
//
//    val ignorePattern = "Được gửi từ Mocha"
//
//    def standardized_input_string(input: String, default_output: String = ""): String = {
//      val output = input match {
//        case "" | null => default_output
//        case _ => input
//      }
//      output
//    }
//
//    // recorrect character
//    def remove_accents(input: String): String = {
//      val s1 = "ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ"
//      val s0 = "AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy"
//      var output = ""
//      val str = standardized_input_string(input)
//      for (i <- 0 until str.length) {
//        if (s1.contains(str(i))) {
//          output = output + s0(s1.indexOf(str(i)))
//        } else output = output + str(i)
//      }
//      output
//    }
//
//    val removeURLAndPattern = input
//      .replaceAll(ignorePattern, "")
//      .toLowerCase
//      .replaceAll("https?:\\/\\/(?:[-\\w.]|(?:%[\\da-fA-F]{2}))+", "URL")
//
//    val output = remove_accents(removeURLAndPattern)
//      .replaceAll("[^a-zA-Z0-9]", " ")
//      .replaceAll(" +", " ")
//      .replaceAll("[o0-9]", "#")
//
//    output
//  }
//
//  def udfCleanText = udf((smsContent: String) => cleanText(smsContent))
//
//  def udfCountPattern = udf((smsContent: String, pattern: String) => countPatternInContent(smsContent, pattern))
//
//  def countPatternInContent(content: String, pattern: String): Int = {
//    val size = pattern.r.findAllMatchIn(content).size
//    size
//  }
//
//  def concatStringArray(ngams: Seq[String]*): Seq[String] = {
//    var output = new ArrayBuffer[String]()
//
//    for (locationlist <- ngams) {
//      output ++= locationlist.filter(_ != "")
//    }
//    output.toSet.toSeq
//  }
//
//  def transformNgamData(data: DataFrame): DataFrame = {
//    val ngram1 = new NGram()
//      .setN(1)
//      .setInputCol("words")
//      .setOutputCol("ngram1")
//
//    val ngram2 = new NGram()
//      .setN(2)
//      .setInputCol("words")
//      .setOutputCol("ngram2")
//
//    val ngram3 = new NGram()
//      .setN(3)
//      .setInputCol("words")
//      .setOutputCol("ngram3")
//
//    def udfConcatArray = udf((ngam1: Seq[String], ngam2: Seq[String], ngam3: Seq[String]) => concatStringArray(ngam1, ngam2, ngam3))
//
//    val output = ngram1
//      .transform(ngram2.transform(ngram3.transform(data)))
//      .withColumn("ngram", udfConcatArray(col("ngram1"), col("ngram2"), col("ngram3")))
//      .drop("ngram1", "ngram2", "ngram3")
//
//    output
//  }
//
//  def getProbability(vector: Vector): Double = {
//    vector.toArray(1)
//  }
//
//  def udfProbability = udf((vector: Vector) => getProbability(vector))
//
//  object CountVectorBroadCast {
//
//    @volatile private var instance: Broadcast[CountVectorizerModel] = null
//    @volatile private var lastUpdated = 0L
//
//    def getInstance(sc: SparkContext, timeUpdated: Long): Broadcast[CountVectorizerModel] = {
//      if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
//        synchronized {
//          if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
//            val cvModel = CountVectorizerModel.load("streaming/cvModel-updated")
//            lastUpdated = timeUpdated
//            instance = sc.broadcast(cvModel)
//          }
//        }
//      }
//      instance
//    }
//  }
//
//  object LogisticBroadCast {
//
//    @volatile private var instance: Broadcast[LogisticRegressionModel] = null
//    @volatile private var lastUpdated = 0L
//
//    def getInstance(sc: SparkContext, timeUpdated: Long): Broadcast[LogisticRegressionModel] = {
//      if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
//        synchronized {
//          if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
//            val lr = LogisticRegressionModel.load("streaming/classifier-updated")
//            lastUpdated = timeUpdated
//            instance = sc.broadcast(lr)
//          }
//        }
//      }
//      instance
//    }
//  }
//
//  object SparkSessionSingleton {
//    @transient private var instance: SparkSession = _
//
//    def getInstance(sparkConf: SparkConf): SparkSession = {
//      if (instance == null) {
//        instance = SparkSession
//          .builder
//          .config(sparkConf)
//          .getOrCreate()
//      }
//      instance
//    }
//  }
//
//  def firstExample(): Unit = {
//    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("test_stream")
//    val ssc = new StreamingContext(conf, Seconds(20))
//    ssc.sparkContext.setLogLevel("ERROR")
//    ssc.checkpoint("checkpoint")
//
//    val lines = ssc.socketTextStream("localhost", 9999)
//
//    lines.foreachRDD(rdd => {
//
//      // if time > 6s then broadcast
//      val cvBrd = CountVectorBroadCast.getInstance(rdd.sparkContext, System.currentTimeMillis())
//
//      val lrBrd = LogisticBroadCast.getInstance(rdd.sparkContext, System.currentTimeMillis())
//
//      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
//      import spark.implicits._
//
//      val warning = rdd.toDF("content_raw")
//        .withColumn("content", udfCleanText(col("content_raw")))
//        .withColumn("words", split(col("content"), " "))
//        .withColumn("length_sms", size(col("words")))
//        .withColumn("count_phone", udfCountPattern(col("content"), lit("#########")))
//        .withColumn("count_equal", udfCountPattern(col("content"), lit("=")))
//        .filter(col("length_sms") > 5)
//
//      val ngramDF = transformNgamData(warning)
//      val dataFeature = cvBrd.value.transform(ngramDF)
//
//      val assembler = new VectorAssembler()
//        .setInputCols(Array("cv-feature", "length_sms", "count_phone", "count_equal"))
//        .setOutputCol("feature")
//      val dataTrain = assembler.transform(dataFeature)
//      val dataTransform = lrBrd.value.transform(dataTrain)
//        .where("prediction = 1.0")
//        .withColumn("simso", udfProbability(col("probability")))
//        .where("simso > 0.8")
//        .select("content_raw")
//
//      dataTransform
//        .foreachPartition(par => {
//          val jedis = new Jedis("localhost", 6379)
//          while (par.hasNext) {
//            val key = "simso" + "_" + par.next().getAs[String]("content_raw")
//            jedis.set(key, "1")
//          }
//          jedis.close()
//        })
//    })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def main(args: Array[String]): Unit = {
//    firstExample()
//  }
//
//}
