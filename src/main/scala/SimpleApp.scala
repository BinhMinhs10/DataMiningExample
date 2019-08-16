import org.apache.spark.sql.{SparkSession}
import java.lang.Math
import org.apache.spark.sql.functions._


object SimpleApp {
  def main(args: Array[String]) {


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Simple Application")
      .getOrCreate()

    val logFile = "/home/binhminhs10/Documents/pom.xml" // Should be some file on your system
    val logData = spark.read.textFile(logFile).cache()

    val first_line = logData.first()
    val linesWithSpark = logData.filter(line => line.contains("spark")).count()
    println(s"Lines with spark: $linesWithSpark")




    // recorrect character
    def remove_accents(input: String): String = {
      val s1 = List("qn", "hn")
      val s0 = List("Quảng ninh", "hà nội")
      var output = ""
      val words = input.split(" ")

      for (i <- 0 until words.length ) {
        if (s1.contains(words(i))) {
          output = output + s0(s1.indexOf(words(i))) + "_"
        } else output = output + words(i) + "_"
      }
      output
    }
    val test = remove_accents("than bùn qn mới")
    println(test)

    // case
    def matchTest(x: Int): String = x match {
      case 1 => "ha giang"
      case 2 => "two"
      case _ => "other"
    }
    matchTest(4)



    val data = List(("Smith","","Smith","36636","M",60000),
      ("Michael ","Rose","","40288","M",70000),
      ("Robert cuc canh sat","","Williams","42114","",400000),
      ("Maria","Maria","Jones","39192","F",500000),
      ("Jen","Mary","Brown","0","F",0))

    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
    val df = spark.createDataFrame(data).toDF(cols:_*)
    val df2 = df.withColumn("new_gender", when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown"))
    df2.show()

    import spark.implicits._

    def ranking(fname: String, lname: String, dob: Int, salary: Int): Int ={
      var output = 0
      if (fname == lname){
        output = output + 2
      } else if (fname.contains("cuc canh sat")){
        output = output + 1
      }
      if (dob == salary){
        output = output + 1
      }
      output
    }
    val udfRanking = udf((fname: String, lname: String, dob: Int, salary: Int) => ranking(fname, lname, dob, salary))
    val df3 = df.withColumn("rank", udfRanking($"first_name", $"last_name", $"dob",$"salary"))
    df3.show()

  }
}