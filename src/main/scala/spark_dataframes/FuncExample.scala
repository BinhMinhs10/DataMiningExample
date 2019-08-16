package spark_dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._


import org.apache.log4j.Logger
import org.apache.log4j.Level

object FuncExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Read parquet
    val input_path = "src/main/resources/userdata1.parquet"
    val df_input = spark.read.parquet(input_path)
    df_input.filter("id in (25,34,50)").show(20,false)

    // Read csv =========================================================
    val path = "src/main/resources/2017-financial-year.csv"
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(path)
    df.show(10,false)

    //
    import spark.implicits._
    val schema = List("index", "value")
    val row1 = ("1", 100)
    val row2 = ("2", 200)
    val myself_df = Seq(row1, row2).toDF(schema:_*)

    myself_df.show()

    df_input.createOrReplaceTempView("df_input")
    val my_df = spark.sql("SELECT * From df_input Where country='China'")
    //my_df.show(20, false)
    my_df.printSchema()

    my_df.select(avg("salary")).show()
    my_df.select(max("salary")).show()

    println("Group by country value")
    df_input.groupBy("country").count().orderBy("country").filter("count > 5").show(10)

    import java.time._
    import java.time.format.DateTimeFormatter

    val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val date_int_format = DateTimeFormatter.ofPattern("mm:HH")
    val last_extract_value="2018-05-09 10:04:25.375"
//    last_extract_value: String = 2018-05-09 10:04:25.375
    //String to Date objects
    val string_to_date = datetime_format.parse(last_extract_value)

    println(date_int_format.format(string_to_date))
  }
}
