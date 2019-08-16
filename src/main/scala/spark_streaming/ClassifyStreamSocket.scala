package spark_streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ClassifyStreamSocket {


  object LogisticBroadCast {

    @volatile private var instance: Broadcast[PipelineModel] = null
    @volatile private var lastUpdated = 0L

    def getInstance(sc: SparkContext, timeUpdated: Long): Broadcast[PipelineModel] = {
      if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
        synchronized {
          if (instance == null || timeUpdated - lastUpdated > 6 * 1000) {
            val lModel = PipelineModel.load("tmp/steaming/logisticRegression")
            println("Loaded model")
            lastUpdated = timeUpdated
            instance = sc.broadcast(lModel)
          }
        }
      }
      instance
    }
  }

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  def firstExample(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test_stream")

    val ssc = new StreamingContext(conf, Seconds(20))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

    lines.foreachRDD(rdd => {

      // if time > 6s then broadcast
      val lrBrd = LogisticBroadCast.getInstance(rdd.sparkContext, System.currentTimeMillis())

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._


      // Make predictions on test documents.
      lrBrd.value.transform(rdd.toDF("text"))
        .select( "text", "probability", "prediction").show()



    })
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    firstExample()
  }
}
