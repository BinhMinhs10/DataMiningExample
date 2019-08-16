package tast_statistic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object ExampleSMS {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val df_log = spark.read
      .option("delimiter","\t")
      .option("header", "false")
      .csv("00_brandname*")
      .toDF("id", "creation_date", "closed_date", "from", "to", "owner_userid", "answer_count","time", "content")
    df_log.sort("from").show(20, false)

//    val count = df_log.count()
//    println("Number of log: "+ count)

    df_log.printSchema()
    df_log.select("from","to","time",  "content").show(10)

    // where condition
    //df_log.filter("from == 'SMS NHO'").show(10)

    // Group by with filter
    val countContent = df_log.select("from","to","time",  "content")
      .groupBy("from", "content")
      .agg(
        count(col("content")).alias("count_content"),
        countDistinct(col("to")).alias("count_dt_to")
      )
      .filter(col("count_dt_to")>= 5)
    countContent.show()
  }
}
