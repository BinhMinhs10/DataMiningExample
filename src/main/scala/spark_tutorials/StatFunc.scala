package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{avg, max, mean}

object StatFunc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val dfQuestionsCSV = spark.read
      .option("header", "true")
      .option("dataFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("questions_10K.csv")
      .toDF("id","creation_date", "closed_date", "deletion_date", "score", "owner_userid","answer_count")
    dfQuestionsCSV.printSchema()

    val dfQuestions = dfQuestionsCSV.select(
      dfQuestionsCSV.col("id").cast("integer"),
      dfQuestionsCSV.col("creation_date").cast("timestamp"),
      dfQuestionsCSV.col("closed_date").cast("timestamp"),
      dfQuestionsCSV.col("deletion_date").cast("date"),
      dfQuestionsCSV.col("score").cast("integer"),
      dfQuestionsCSV.col("owner_userid").cast("integer"),
      dfQuestionsCSV.col("answer_count").cast("integer")
    )

    val dfTags = spark.read
      .option("header", "true")
      .csv("question_tags_10K.csv")
    dfTags.show(10)

    // DataFrame Statistics using describe() method
    val dfQuestionsStatistics = dfQuestions.describe()
    dfQuestionsStatistics.show()

    /* correlation between col */
    val correlation = dfQuestions.stat.corr("score", "answer_count")
    println(s"correlation between column score and answer_count = $correlation")

    val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
    dfFrequentScore.show(false)

    // display a tabular view of score
    val dfScoreByUserid = dfQuestions.filter("owner_userid > 0 and owner_userid < 20")
      .stat
      .crosstab("score", "owner_userid")
    dfScoreByUserid.show(10)


    val dfQuestionsByAnswerCount = dfQuestions
      .filter("owner_userid > 0")
      .filter("answer_count in (5, 10, 20)")

    // count how many rows match answer_count in (5, 10, 20)
    dfQuestionsByAnswerCount
      .groupBy("answer_count")
      .count()
      .show()

    /* sampleBy method get random example follow scale */
    val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)
    dfQuestionsByAnswerCount
      .stat
      .sampleBy("answer_count", fractionKeyMap, 37L)
      .groupBy("answer_count")
      .count()
      .show()

    // Bloom Filter (
    // When train ML pipeline using bloom filter to pre-processing
    // example create filter tags  1000 item with 10% false positive
    val tagsBloomFilter = dfTags.stat.bloomFilter("tag", 1000L, 0.1)

    println(s"bloom filter contains java tag = ${tagsBloomFilter.mightContain("java")}")
    println(s"bloom filter contains some unknown tag = ${tagsBloomFilter.mightContain("unknown tag")}")


    val cmsTag = dfTags.stat.countMinSketch("tag", 0.1, 0.9, 37)
    val estimatedFrequency = cmsTag.estimateCount("java")
    println(s"Estimated frequency for tag java = $estimatedFrequency")

    // without replacement it mean can't repeat pick
    // 12, 13, 14, 15, 16, 17, or 18
    // after pick 12 with 1/7 probability. at this point there are only six possibilities
    val dfTagsSample = dfTags.sample(true, 0.2, 3L)
    println(s"Number of rows in sample dfTagsSample = ${dfTagsSample.count()}")
    println(s"Number of rows in dfTags = ${dfTags.count()}")





  }
}
