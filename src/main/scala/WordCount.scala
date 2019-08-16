import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object WordCount {
  def main(args: Array[String]): Unit ={
    val sc = new SparkContext("local", "Word Count", "/home/binhminhs10/spark")
    val input = sc.textFile("/home/binhminhs10/spark/text.txt").cache()
    val count = input.count() // Number of items in this Dataset
    val sizeMax = input.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
    println(s"Max word in row: $sizeMax")


    println("OK----------")
    println(s"number word in file: ${count}")
  }
}
