package spark_streaming

import org.apache.spark._
import org.apache.spark.streaming._

object TCPsocket {

  def main(args: Array[String]): Unit = {
    // local[n] is n working thread always n > receivers
    // master: is Spark, Mesos or YARN cluster URL
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetwordWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // discretized stream or DStream: seq of RDDs from Kafka, Flume, Kinesis
    // run NetCat : $nc -lk 9999
    //
    val lines = ssc.socketTextStream("localhost", 9999)
    // flatMap map one to many
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    println(wordCount.print())

    ssc.start() // start to computation
    ssc.awaitTermination() // wait for computation

    // Monitor dataDirectory and process any files created in that
    /* Must:
    * same format
    * in dataDirectory not nested directories*/
    // streamingContext.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)

  }
}
