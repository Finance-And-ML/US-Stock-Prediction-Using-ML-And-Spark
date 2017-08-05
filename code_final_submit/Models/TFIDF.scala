import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
// import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.feature.PCA

//Simple TF-IDF Model for Sentiment Analysis

//val articleDF = sqlCtx.read.json("/Users/jimmy/Desktop/tmp/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/2017-07-01.json")
//

val target_labeled_df = sqlContext.read.json("/user/cyy292/project/target_labeled.json")

val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
val wordsData = tokenizer.transform(target_labeled_df)
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(80000)
val featurizedData = hashingTF.transform(wordsData)
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("IDF_features")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
// rescaledData.select("features").take(3).foreach(println)

// val pca = new PCA().setInputCol("IDF_features").setOutputCol("pcaFeatures").setK(1000).fit(rescaledData)
// val pcaFeatures = pca.transform(df).select("pcaFeatures")

//get weight fron SparseVector
val featureVec = udf{x:Vector=>x.toArray}
val rescaledData_weight = rescaledData.withColumn("features_weight", featureVec(rescaledData("IDF_features")))
rescaledData_weight.select("features_weight").take(3).foreach(println)
