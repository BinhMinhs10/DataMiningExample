package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.reflect.io.File

object SparkSql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
//      .config("spark.sql.warehouse.dir", )
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sms = spark
      .read
      .option("delimiter",";")
      .csv("sms.txt")
      .select("_c0","_c1","_c2" )
      .toDF("from", "to", "time")

    sms.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("testcxv")

  }
}
