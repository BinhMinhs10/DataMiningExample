package spark_streaming

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ErrorExample {

  // must delete folder in hadoop
  private val checkpointDir = "hdfs://localhost:9000/checkpoint-error-example"

  def main(args: Array[String]): Unit = {
    val streamContext = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_test_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)

    )

    val wordParis = stream.map(record => (record.value(), 1))
    val totalCount = wordParis.countByWindow(Seconds(30), Seconds(15))
    totalCount.print()

    // The second computation
    val wordCount = stream.map(record => record.value()).countByValueAndWindow(Seconds(30), Seconds(15))
    wordCount.print()

    stream.foreachRDD{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamContext.start()
    streamContext.awaitTermination()

  }

  private def createStreamingContext(): StreamingContext = {
    val sparkConfig = new SparkConf().setMaster("local[4]").setAppName("Spark Kafka Test")
    val streamingContext = new StreamingContext(sparkConfig, Seconds(5))
    streamingContext.checkpoint(checkpointDir)
    streamingContext
  }
}
