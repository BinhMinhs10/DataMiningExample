package spark_tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._

/*
* dataset: stackoverflow
* link: http://allaboutscala.com/big-data/spark/
* */
object DFQuery {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val dfTags = spark.read
      .option("header", "true")
      .csv("question_tags_10K.csv")

    val dfQuestionsCSV = spark.read
      .option("header", "true")
      .option("dataFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("questions_10K.csv")
      .toDF("id","creation_date", "closed_date", "deletion_date", "score", "owner_userid","answer_count")
    dfQuestionsCSV.printSchema()


    val dfQuestions = dfQuestionsCSV.select(
      dfQuestionsCSV.col("id").cast("integer"),
      dfQuestionsCSV.col("creation_date").cast("timestamp"),
      dfQuestionsCSV.col("closed_date").cast("timestamp"),
      dfQuestionsCSV.col("deletion_date").cast("date"),
      dfQuestionsCSV.col("score").cast("integer"),
      dfQuestionsCSV.col("owner_userid").cast("integer"),
      dfQuestionsCSV.col("answer_count").cast("integer")
    )

    dfQuestions.printSchema()

    // need to import implicit covert Scala object to dataframe
    import spark.implicits._
    dfQuestions
      .withColumn("idIsNULL", when($"owner_userid".isNull, 0).otherwise(1))
      .show(10, false)
    dfQuestions.filter("owner_userid is null").show()


    val dfQuestionsSubset = dfQuestions.filter("score > 400 and score <410").toDF()
    dfQuestionsSubset.join(dfTags, "id")
      .select("owner_userid","tag", "creation_date", "score")
      // drop row have null value
      .na.drop()
      .show(10)


    dfTags.createOrReplaceTempView("so_tags")
    spark.catalog.listTables().show()
    spark.sql("show tables").show()

    spark.sql("select count(*) as php_count from so_tags where tag='php'".stripMargin).show(10)

    /* User define function (UDF): append prefix so to tags column*/
    def prefixStackoverflow(s: String): String = s"so_$s"

    // Register User Defined Function (UDF)
    spark
      .udf
      .register("prefix_so", prefixStackoverflow _)

    // Use udf prefix_so to augment each tag has a prefix so_
    spark
      .sql("""select id, prefix_so(tag) from so_tags""".stripMargin)
      .show(10)


    dfQuestions
      .select(mean("score"))
      .show()

    dfQuestions.filter("id > 400 and id < 450")
      .filter("owner_userid is not null")
      .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
      .groupBy(dfQuestions.col("owner_userid"))
      .agg(
        avg("score").alias("average score"),
        max("answer_count").alias("max answer_count"))
      .show()

    // DataFrame Statistics using describe() method
    val dfQuestionsStatistics = dfQuestions.describe()
    dfQuestionsStatistics.show()


    // create DataFrame from collection
    val seqTags = Seq(
      1 -> "so_java",
      1 -> "so_jsp",
      2 -> "so_erlang",
      3 -> "so_scala",
      3 -> "so_akka"
    )

    // need to import implicit covert Scala object to dataframe
    import spark.implicits._
    val dfMoreTags = seqTags.toDF("id", "tag")

    // DataFrame Union
    val dfUnionOfTags = dfTags
      .union(dfMoreTags)
      .filter("id in (1,3)")
    dfUnionOfTags.show()
    // DataFrame intersection
    val dfIntersectionTags = dfMoreTags
      .intersect(dfUnionOfTags)
      .show(10)

    // Append column to Dataframe using withColumn()
    val dfSplitColumn = dfMoreTags
      .withColumn("tmp", split($"tag", "_"))
      .select(
        $"id",
        $"tag",
        $"tmp".getItem(0).as("so_prefix"),
        $"tmp".getItem(1).as("so_tag")
      ).drop("tmp")
    dfSplitColumn.show(10)


    /* Create dataframe from Tuple*/
    val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    val df = spark
      .createDataFrame(donuts)
      .toDF("Donut Name", "Price")
    df.withColumn("Contains plain", instr($"Donut Name", "donut"))
      .withColumn("Length", length($"Donut Name"))
      .withColumn("Trim", initcap(trim($"Donut Name")))
      .withColumn("LTrim", ltrim($"Donut Name"))
      .withColumn("RTrim", rtrim($"Donut Name"))
      .withColumn("Reverse", reverse($"Donut Name"))
      .withColumn("Substring", substring($"Donut Name", 0, 5))
      .withColumn("IsNull", isnull($"Donut Name"))
      .withColumn("Concat", concat_ws(" - ", $"Donut Name", $"Price"))
      .withColumn("InitCap", initcap($"Donut Name"))
      .show()


    val columnNames: Array[String] = df.columns
    columnNames.foreach(name => println(s"$name"))


    /* Json into DataFrame using explode()*/
    // ERROR


    val tagsDF = spark
      .read
      .option("multiLine", true)
      .option("inferSchema", true)
      .json("tags_sample.json")

    tagsDF.printSchema()
    //val df_Fromjson = tagsDF.select( explode($"stackoverflow") as "stackoverflow_tags")


    val targets = Seq(("Plain Donut", Array(1.50, 2.0), "2018-04-17"), ("Vanilla Donut", Array(2.0, 2.50), "2018-04-01"), ("Strawberry Donut", Array(2.50, 3.50), "2018-04-02"))
    val df_splitArr = spark
      .createDataFrame(targets)
      .toDF("Name", "Prices", "Purchase Date")


    var df2 = df_splitArr
      .select(
        $"Name",
        $"Purchase Date",
        $"Prices"(0).as("Low Price"),
        $"Prices"(1).as("High Price")
      )

    // User Defined Function (UDF)
    val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
      case "plain donut"    => Seq(100, 500)
      case "vanilla donut"  => Seq(200, 400)
      case "glazed donut"   => Seq(300, 600)
      case _                => Seq(150, 150)
    }

    val udfStockMinMax = udf(stockMinMax)
    df2 = df2.withColumn("Stock Min Max", udfStockMinMax($"name"))


    df2 = df2
      // using lit to create constant column
        .withColumn("Tasty", lit(true))
      // Rename DF column
        .withColumnRenamed("name", "Donut Name")
        // 2 deciaml places
        .withColumn("Price Formatted", format_number($"High Price", 2))
        .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
        .withColumn("Name Uppercase", upper($"Donut Name"))
        .withColumn("Name Lowercase", lower($"Donut Name"))
        .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
        .withColumn("Day", dayofmonth($"Purchase Date"))
        .withColumn("dow", date_format($"Purchase Date","uuu"))
        .withColumn("Dowe", date_format($"Purchase Date","E"))
        .withColumn("Month", month($"Purchase Date"))
        .withColumn("Year", year($"Purchase Date"))
        .withColumn("MD5", md5($"Donut Name"))
    df2.show()

    val firstRow = df.first()
    val name = firstRow.get(0)
    val price = firstRow.getAs[Double]("Price")
    println(s"First row column price = $price")

    /* Format DataFrame column*/


  }
}
