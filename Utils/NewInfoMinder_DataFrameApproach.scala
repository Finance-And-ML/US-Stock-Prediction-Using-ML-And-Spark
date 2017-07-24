import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, NGram, StopWordsRemover, VectorAssembler}

case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/20170717.json")
val alias2ticker = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/alias2ticker.json")
val tickerInfo = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/tickerInfo.json")

val newsds = newsdf.where("keywords != null")

val tokenizer = new Tokenizer().setInputCol("news_title").setOutputCol("news_title_tk")
val news_title_tk = tokenizer.transform(newsds)

val remover = new StopWordsRemover().setInputCol("news_title_tk").setOutputCol("news_title_clean")
val news_title_clean_ds = remover.transform(news_title_tk)

val ngram_2 = new NGram().setN(2).setInputCol("news_title_clean").setOutputCol("news_title_ngrams_2")
val ngram_2_trans = ngram_2.transform(news_title_clean_ds)

val ngram_3 = new NGram().setN(3).setInputCol("news_title_clean").setOutputCol("news_title_ngrams_3")
val ngram_3_trans = ngram_3.transform(ngram_2_trans)

val ngram_4 = new NGram().setN(4).setInputCol("news_title_clean").setOutputCol("news_title_ngrams_4")
val ngram_4_trans = ngram_4.transform(ngram_3_trans)

case class Alias2TickerRow(alias:String, ticker:String)
val alias2ticker_ds: Dataset[Alias2TickerRow] = alias2ticker.as[Alias2TickerRow]
case class News_title_tk_cleaned_ngram_CC(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String, news_title_tk:Array[String], news_title_clean:Array[String], news_title_ngrams_2:Array[String], news_title_ngrams_3:Array[String], news_title_ngrams_4:Array[String])
val news_ds: Dataset[News_title_tk_cleaned_ngram_CC] = ngram_4_trans.as[News_title_tk_cleaned_ngram_CC]

val news_title_ngrams_only = news_ds.map(s => s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4)

val title_alias_ticker_tuple = news_title_ngrams_only.collect().map(one_set_of_keywords => (one_set_of_keywords, alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias))))

// In order to view result:
sc.parallelize(title_alias_ticker_tuple.map(s => s._1.mkString(" ") + " alias-ticker-pair:" +s._2.mkString(" ")).toSeq).saveAsTextFile("title_alias_ticker_temp_result")
sc.parallelize(title_alias_ticker_tuple.map(s => s._2.mkString(" ")).toSeq).saveAsTextFile("alias_ticker_only_temp_result")
