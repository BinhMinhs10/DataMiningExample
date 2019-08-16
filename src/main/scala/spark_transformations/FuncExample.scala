package spark_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FuncExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTransformation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val babyNames = sc.textFile("src/main/resources/baby_names.csv")
    // Transform RDD => RDD of array of strings
    val rows = babyNames.map(line => line.split(","))




  }
}
