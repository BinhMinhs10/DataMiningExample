package tast_statistic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FrequencySDT {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sms = spark
      .read
      .option("delimiter",";")
      .csv("sms.txt")
      .select("_c0","_c1","_c2" )
      .toDF("from", "to", "time")
    sms.show()



    // cach 1 =====================================

    val dfFrom = sms.select("from")
      .withColumnRenamed("from", "phone")
      .union(sms.select("to").withColumnRenamed("to", "phone"))
      .groupBy("phone")
      .agg(count("phone").alias("count"))
      .orderBy(desc("count"))

    dfFrom.show(10)

    // Top 10 sdt sms frequency cach 2 dai hon
    val countfrom = sms
      .groupBy("from")
      .agg(
        count(col("to")).alias("count")
      )
        .withColumnRenamed("from", "phone")
    countfrom.show()
    val countto = sms
      .groupBy("to")
      .agg(
        count(col("from")).alias("count")
      )
      .withColumnRenamed("to", "phone")

    countto.show()

    val top10 = countfrom.union(countto)
      .groupBy("phone")
      .agg(
        sum("count").alias("top")
      )
        .orderBy(desc("top"))
    top10.show(10, false)

    /*countfrom
      .join(countto, countfrom.col("from").equalTo(countto("to")))
    countfrom.show(false)*/

  }
}
