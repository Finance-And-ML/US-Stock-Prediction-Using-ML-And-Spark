import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

# Simple TF-IDF Model for Sentiment Analysis

val articleDF = sqlCtx.jsonFile("/user/cyy292/project/wsjArticle/*")

val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
val wordsData = tokenizer.transform(articleDF)
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
val featurizedData = hashingTF.transform(wordsData)
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
// rescaledData.select("features", "label").take(3).foreach(println)

val genData = rescaleData.rdd
