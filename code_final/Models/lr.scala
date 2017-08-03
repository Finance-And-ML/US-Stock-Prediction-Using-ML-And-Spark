import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.param
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
// val inputData = spark.read.format("libsvm").load("/Users/jimmy/Desktop/tmp/mllib/sample_libsvm_data.txt")

val feature_merge = udf((x: Vector, y: Seq[Double], z: Seq[Double]) => Vectors.dense(x.toArray ++ y ++ z))
// val feature_merge = udf((x:Vector) => x.toArray)

val input = rescaledData.withColumn("features", feature_merge(col("IDF_features"), col("sector_features"), col("category_features"))).withColumnRenamed("label_7m", "label")
// val input = rescaledData.withColumn("features", feature_merge(col("IDF_features")))

val Array(training, test) = input.randomSplit(Array(0.8, 0.2))
val lr = new LogisticRegression().setFitIntercept(false).setLabelCol("label").setFeaturesCol("features")

//set parameters
val paramGrid = new ParamGridBuilder().addGrid(lr.elasticNetParam, Array(0.0 ,1.0)).addGrid(lr.regParam, Array(0.1, 0.01, 0.001)).build()

// instantiate the One Vs Rest Classifier.
val ovr = new OneVsRest().setClassifier(lr)


// CrossValidation
val cv = new CrossValidator().setEstimator(ovr).setEvaluator(new MulticlassClassificationEvaluator()).setEstimatorParamMaps(paramGrid)
// val cv = new CrossValidator().setEstimator(ovr).setEvaluator(new MulticlassClassificationEvaluator()).setEstimatorParamMaps(paramGrid).setNumFolds(3)
//training
val cvModel = cv.fit(training)
// val lrModel = ovr.fit(training)

val predictions = cvModel.transform(test)

val eval_f1 = new MulticlassClassificationEvaluator()
val eval_precision = new MulticlassClassificationEvaluator().setMetricName("precision")
val eval_recall = new MulticlassClassificationEvaluator().setMetricName("recall")
// compute the classification error on test data.
print(eval_f1.evaluate(predictions))
print(eval_precision.evaluate(predictions))
print(eval_recall.evaluate(predictions))

//best model parameter
cv.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1
