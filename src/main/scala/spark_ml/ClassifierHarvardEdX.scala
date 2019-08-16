package spark_ml

import sys.process._
import scala.language.postfixOps
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType



/*
* https://towardsdatascience.com/training-your-first-classifier-with-spark-and-scala-893d7c6f7d88
* */
object ClassifierHarvardEdX {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val data = spark.read
      .option("header","true")
      .csv("data/mooc.csv")

    data.show()

    import spark.implicits._

    val df = (data.select(data("certified").as("label").cast(DoubleType),
            $"registered", $"viewed".cast("integer"),
            $"explored".cast("integer"),
            $"final_cc_cname_DI", $"gender", $"nevents".cast("integer"),
            $"ndays_act".cast("integer"), $"nplay_video".cast("integer")
            , $"nchapters".cast("float"), $"nforum_posts".cast("integer")))
    df.printSchema()



    // convert string to integer
    val indexer1 = new StringIndexer()
      .setInputCol("final_cc_cname_DI")
      .setOutputCol("countryIndex")
//      .setHandleInvalid("keep")

    val indexed1 = indexer1.fit(df).transform(df)

    val indexer2 = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("genderIndex")
//      .setHandleInvalid("keep")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1)

    // one hot encoding
    val encoder1 = new OneHotEncoder()
      .setInputCol("countryIndex")
      .setOutputCol("countryVec")
    var encoded1 = encoder1.transform(indexed2)

    encoded1.show()

    encoded1 = encoded1.na.drop()

    val nanEvents = encoded1.groupBy("nevents").count().orderBy( $"count".desc)
    nanEvents.show()




    val assembler = new VectorAssembler().setInputCols(
      Array(
        "viewed", "explored", "nevents", "ndays_act", "nplay_video",
        "nchapters", "nforum_posts", "countryVec"))
      .setOutputCol("features")

    // Transform the DataFrame
    val output = assembler.transform(encoded1).select($"label",$"features")
    val Array(training, test) = output.select("label","features").
      randomSplit(Array(0.7, 0.3), seed = 12345)

    val rf = new RandomForestClassifier()

    // create the param grid
    val paramGrid = new ParamGridBuilder().
      addGrid(rf.numTrees,Array(20,50,100)).
      build()

//    val model = rf.fit(training)
//    val results = model.transform(test).select("features", "label", "prediction")
//    val predictionAndLabels = results.
//      select($"prediction",$"label").
//      as[(Double, Double)].
//      rdd

    // create cross val object, define scoring metric
    val cv = new CrossValidator().
      setEstimator(rf).
      setEvaluator(new MulticlassClassificationEvaluator().setMetricName("weightedRecall")).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(3)

    // You can then treat this object as the model and use fit on it.
    val models = cv.fit(training)

    val best_lr = models.bestModel
    val results = best_lr.transform(test).select("features", "label", "prediction")
    val predictionAndLabels = results.
      select($"prediction",$"label").
      as[(Double, Double)].
      rdd



    // confusion matrix
    // Instantiate a new metrics objects
    val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
    val mMetrics = new MulticlassMetrics(predictionAndLabels)
    val labels = mMetrics.labels

    // Print out the Confusion matrix
    println("Confusion matrix:")
    println(mMetrics.confusionMatrix)

    labels.foreach { l =>
      println(s"Precision($l) = " + mMetrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + mMetrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
    }

    // metrics such as AUC and AUPRC
    // Precision by threshold
    val precision = bMetrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = bMetrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = bMetrics.pr

    // F-measure
    val f1Score = bMetrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = bMetrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = bMetrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = bMetrics.roc

    // AUROC
    val auROC = bMetrics.areaUnderROC
    println("Area under ROC = " + auROC)


  }
}
