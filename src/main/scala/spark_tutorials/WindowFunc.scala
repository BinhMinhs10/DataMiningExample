package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunc {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("Windown function")
//    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._

    val customers =
      List(("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))
      .toDF("name", "date", "amountSpent")



    // create a window spec by customer and order
    // window starting from -1 and ending at 1
    val wSpec1 = Window.partitionBy("name")
      .orderBy("date")
      .rowsBetween(-1,1)

    customers.withColumn("movingAvg", avg(customers("amountSpent")).over(wSpec1)).show()

//    customers.show()

    // Cumulative Sum
    val wSpec2 = Window.partitionBy("name")
      .orderBy("date")
      .rowsBetween(Long.MinValue, 0)

    // create a new which calculate the sum
    customers
      .withColumn("curmSum", sum(customers("amountSpent")).over(wSpec2)).show()

    // Data from previous row
    val wSpec3 = Window.partitionBy("name").orderBy("date")
    customers.withColumn("prevAmountSpent", lag(customers("amountSpent"), 1).over(wSpec3)).show()


    // Rank function
    customers.withColumn("rank", rank().over(wSpec3)).show()

    val set = customers.select("name").collect().map(_(0)).toSet
    for(i <- set){
      println(i+"\n")
    }

    val temp = customers.filter($"name".isin(set.toSeq: _*))
    temp.show()
  }
}
