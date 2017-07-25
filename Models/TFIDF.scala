import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

//Simple TF-IDF Model for Sentiment Analysis

val articleDF = sqlCtx.read.json("/Users/jimmy/Desktop/tmp/2017-07-01.json")
//
val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
val wordsData = tokenizer.transform(articleDF)
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
val featurizedData = hashingTF.transform(wordsData)
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
// rescaledData.select("features").take(3).foreach(println)
val featureVec = udf{x:SparseVector=>x.toArray}
val rescaledData_get = rescaledData.withColumn("features_trans", featureVec(rescaledData("features")))
rescaledData_get.select("features", "features_trans").take(3).foreach(println)
