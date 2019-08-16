package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExampleUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val df = spark.read.option("header", true).csv("data/wine-data.csv")
    df.show(false)

    val featureUDF = udf((x: Double, y: Double) => if(y > 100) {x/math.log10(y)} else 0)

    val joinDF = df
      .withColumn("ratio", featureUDF(col("points"),col("price")))

    joinDF.show()
  }

}
