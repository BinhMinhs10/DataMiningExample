import org.apache.spark.rdd.RDD
class MyClass {
  val field = "Hello"

  def dpStuff(rdd: RDD[String]): RDD[String] = {
    val field_ = this.field
    rdd.map(x => field_ + x)
  }
}
