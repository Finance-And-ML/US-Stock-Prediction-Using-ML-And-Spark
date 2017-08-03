import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

// val article = sqlCtx.jsonFile("/Users/jimmy/Desktop/article_till_0721/*")
val article = sqlContext.read.json("/user/cyy292/project/target_labeled.json")
article.cache

val article_df = article.where("content is not null")

val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
val wordsData = tokenizer.transform(article_df)

val remover = new StopWordsRemover().setInputCol("words").setOutputCol("words_filtered")
val filteredData = remover.transform(wordsData)

case class filterRow(words_filtered:Array[String])
val filter_ds: Dataset[filterRow] = filteredData.select("words_filtered").as[filterRow]

val wordSet = filter_ds.map(row => row.words_filtered).reduce(_ ++ _).toSet
wordSet.size
