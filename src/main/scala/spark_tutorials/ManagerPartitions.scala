package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util
import java.io.File


object ManagerPartitions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._
    val x = (1 to 10).toList
    val numbersDf = x.toDF("numbers")

    // moving data from some partitions to existing partitions
    val numbersDf2 = numbersDf.coalesce(2)
    println(numbersDf2.rdd.partitions.size)

    // repartition either increase or decrease partitions
    val homerDf = numbersDf.repartition(6)
    println(homerDf.rdd.partitions.size)

    // function delete file or folder
    def deleteFile(filename: String) = {
      val dir = new File(filename)
      if(dir.exists() && dir.isDirectory){
        for (file <- dir.listFiles()){
          file.delete()
        }
      }
      dir.delete()
    }


    deleteFile("tmp/testPartition")

    numbersDf2.write.csv("tmp/testPartition")


    //    repartition by column
    val people = List(
      (10, "blue"),
      (13, "red"),
      (15, "blue"),
      (99, "red"),
      (67, "blue")
    )
    val peopleDf = people.toDF("age", "color")
    val colorDf = peopleDf.repartition($"color")

  }
}
