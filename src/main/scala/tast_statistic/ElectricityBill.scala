package tast_statistic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex

object ElectricityBill {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val smsBill = spark
      .read
      .option("header",true)
      .option("delimiter","\t")
      .csv("electricity_bill06.csv")

    smsBill.show(10, false)
    println(smsBill.count())

    val brandname = smsBill.groupBy("from")
      .count().alias("count")
    brandname.orderBy("from").show(10,false)

    import java.util.regex.Pattern

    val regex = "((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+\\s*vnd)|((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+\\s*d)|((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+\\,\\d+\\s*vnd)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+\\,\\d+\\s*d)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+\\,\\d+)|((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+\\s*vnd)|((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+\\s*d)|((\\+?|\\-?)\\d+\\.\\d+\\.\\d+\\.\\d+)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+\\s*vnd)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+\\s*d)|((\\+?|\\-?)\\d+\\,\\d+\\,\\d+\\,\\d+)|((\\+?|-?)\\d+\\.\\d+\\.\\d+\\s*vnd)|((\\+?|-?)\\d+\\.\\d+\\.\\d+\\s*d)|((\\+?|-?)\\d+\\.\\d+\\.\\d+)|((\\+?|-?)\\d+\\,\\d+\\,\\d+\\s*vnd)|((\\+?|-?)\\d+\\,\\d+\\,\\d+\\s*d)|((\\+?|-?)\\d+\\,\\d+\\,\\d+)|((\\+?|-?)\\d+\\.\\d+\\s*vnd)|((\\+?|-?)\\d+\\.\\d+\\s*d)|((\\+?|-?)\\d+\\.\\d+)|((\\+?|-?)\\d+\\,\\d+\\s*vnd)|((\\+?|-?)\\d+\\,\\d+\\s*d)|((\\+?|-?)\\d+\\,\\d+)|((\\+?|\\-?)\\d{4,9}\\s*vnd)|((\\+?|\\-?)\\d{4,9}\\s*d)|((\\+?|\\-?)\\d{4,15})"
//    val mydata = "\nSHOULD NOT MATCH:\nQuy khach da tra tien thanh cong hoa don tien Dien 973.134 cho PK04000001555 123.232.321 va 123.123.323.123\nChi tiet hoa don thanh toan tien dien cua khach hang PC12AA0142754 la: Khong co thong tin.\t3225\n\tTK33010000239003 tai BIDV -534,589VND vao 12:39 12/06/2018. So du:13,121,920VND. ND: PA14HA0009548_TT tien dien 062018 Nong Quoc Tien PA14HA0009548 CSD4978 CSC5"
//    val pattern = new Regex(regex)
//    val s = (pattern findAllIn mydata)
//    println((pattern findAllIn mydata).mkString(" "))
    val pat = regex.r
    val rm: String => Array[String] = pat.findAllIn(_).toArray[String]
    val rmUDF = udf(rm)
//    val rmDot: String => String = _.replace(".", "")
//    val rmDotUDF = udf(rmDot)
//    val rmComma: String => String = _.replace(",", "")
//    val rmCommaUDF = udf(rmComma)



    import spark.implicits._

    val money = smsBill
      .withColumn("money", rmUDF($"to"))
    money.show(30,false)

  }
}
