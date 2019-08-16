import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object RDDGuide {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDGuide").setMaster("local")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)

    /*  default 2-4 partitions for each CPU in cluster*/
    val distData = sc.parallelize(data, 10)
    println(s"Sum in data: ${distData.reduce((a,b) => a+b)}")
    println("Distribute data: " + distData.collect().mkString(" "))



    /*  local file system, HDFS, Cassandra, HBase, Amazon S3,..*/
    val distFile = sc.textFile("data.txt").persist()
    /* Using take to get */
    println("print 2 row: \n" + distFile.take(2).mkString("\n"))

    // --------------------------------------------------------------------
    /*
    * map: one-to-one
    * flatMap: one-to-many*/
    /* Map action*/
    val Lenght = distFile.map(s => s.length)
    println("Number row in file " + Lenght.count())

    /* Reduce action: total number of characters*/
    val totalLength = Lenght.reduce((a, b) => a+b)
    println("Size of all line: "+ totalLength)

    val count = distFile.map(s => s.length).reduce((a,b) => a+b)
    println(s"Size of all line $count")

    /* flatMap transformation
    * -----------------------------------------------------------------------*/
    val counts = distFile.flatMap(line => line.split(" ").map(word => (word,1))).reduceByKey(_ + _)
    println("word count: " + counts.collect().mkString(" "))

    /* Filter Transformation like where clause SQL*/
    // filter out length(word) is more than 7
    val lg5 = distFile.filter(line => !line.contains("=")).flatMap(line => line.split(" ")).filter(_.length > 7)
    println("word count more than 5: " + lg5.collect().mkString(","))
    // filter(f: (T) ⇒ Boolean): RDD[T]
    val errors = distFile.filter(line => line.contains("ERROR"))

    //val file = "outfile"
    //lg5.saveAsTextFile(file)

    // -----------------------------------------------------------------------
    // mapPartitions
    val parallel = sc.parallelize(1 to 9,3)

    // -----------------------------------------------------------------------
    val myClass = new MyClass()
    myClass.dpStuff(distFile)

    /* Broadcast Variables*/
    val broadcastVar = sc.broadcast(Array(1,2,3))
    println(s"Broadcast value: "+broadcastVar.value.mkString(" "))

    /* Accumulators*/
    var sum = 0
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => sum=sum+x )
    println("sum: "+sum)

    val accum = sc.longAccumulator("My accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    print(s"accumulator: ${accum.value}")



  }
}
