//Local: spark-shell --driver-memory 2G --executor-memory 3G --executor-cores 2
//Dumbo: spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8
//for Scala 2.11 spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
//for Scala 2.10 (Dumbo) $SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, NGram, StopWordsRemover, VectorAssembler}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}

case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val news_data_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/article_till_0721/2017-07-20-*.json"
val alias2ticker_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/alias2ticker.json"
// tickerInfo.json gives 'sector', 'category' and 'group' attributes for each Ticker
val ticker_info_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/tickerInfo.json"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)
val alias2ticker = sqlContext.read.json(alias2ticker_path)
val tickerInfo = sqlContext.read.json(ticker_info_path)

val alias2ticker_ds: Dataset[Alias2TickerRow] = alias2ticker.as[Alias2TickerRow]

val newsds = newsdf.where("keywords is not null")

// Title: Tokenization => Remove Stop Words => N-Gram range[2,4]
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

// Only apply to Reuter News Dataset
/*
case class News_title_tk_cleaned_ngram_CC(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String, news_title_tk:Array[String], news_title_clean:Array[String], news_title_ngrams_2:Array[String], news_title_ngrams_3:Array[String], news_title_ngrams_4:Array[String])
val news_ds: Dataset[News_title_tk_cleaned_ngram_CC] = ngram_4_trans.as[News_title_tk_cleaned_ngram_CC]
val news_title_ngrams_and_keywords_only = news_ds.map(s => s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4 ++ s.keywords.split(","))
*/
// WSJ does not have 'sector' key-value pair
case class News_title_tk_cleaned_ngram_CC(content:String, keywords:String, news_time:String, news_title:String,  url:String, news_title_tk:Array[String], news_title_clean:Array[String], news_title_ngrams_2:Array[String], news_title_ngrams_3:Array[String], news_title_ngrams_4:Array[String])
val news_ds: Dataset[News_title_tk_cleaned_ngram_CC] = ngram_4_trans.as[News_title_tk_cleaned_ngram_CC]
// val news_title_ngrams_and_keywords_only = news_ds.map(s => s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4 ++ s.keywords.split(","))
val news_ds_withKeywordsNgramList = news_ds.map(s => (s.content, s.news_time, s.news_title, s.url, s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4 ++ s.keywords.split(",")))
// To rename column names
val newColumnNames = Seq("content", "news_time", "news_title", "url", "ngramKeywords")
val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF(newColumnNames: _*)


case class News_NGrams_CC(content:String, news_time:String, news_title:String, url:String, ngramKeywords:Array[String])
val new_ds_withNgramWordsArray:Dataset[News_NGrams_CC] = new_df_withNgramWordsArray.as[News_NGrams_CC]
// Proceed the matching - result is in the type of Array[(String, String, String, String, Array[String])]
val result_array_withMatchedTickers_all = new_ds_withNgramWordsArray.collect().map(s => (s.news_title, s.news_time, s.content, s.url, alias2ticker_ds.collect().withFilter(line => s.ngramKeywords.contains(line.alias)).map(line => line.ticker)))
val result_array_withMatchedTickers = result_array_withMatchedTickers_all.withFilter(news => news._5.length != 0).map(news => (news._1, news._2, news._3, news._4, news._5(0)))
// create df/ds from the result
val news_df_withTickersArray = result_array_withMatchedTickers.toSeq.toDF("content", "news_time", "news_title", "url", "tickers")
val news_df_withTickersArray_timestampDate = news_df_withTickersArray.withColumn("news_datetime", (col("news_time").cast("timestamp"))).drop("news_time")

val add15mins = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 15*60*1000))
val add1hr    = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 60*60*1000))
val add2hr    = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 120*60*1000))

val news_final_df = news_df_withTickersArray_timestampDate.withColumn("after_15mins", add15mins(col("news_datetime"))).withColumn("after_1hr", add1hr(col("news_datetime"))).withColumn("after_2hr", add2hr(col("news_datetime")))

case class News_withTickers_CC(content:String, news_title:String, url:String, tickers:String, news_datetime:java.sql.Timestamp, after_15mins:java.sql.Timestamp, after_1hr:java.sql.Timestamp, after_2hr:java.sql.Timestamp)
val news_final_ds:Dataset[News_withTickers_CC] = news_final_df.as[News_withTickers_CC]

//##################################################################################################################################################################
// here stock price comes in
//##################################################################################################################################################################

val stock_price_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Intraday-Dataset/Intraday-Data/20170718_20170721_1minute/alltickers/*.csv"
//dumbo:
// val stock_price_path = "/user/lc3397/project/alltickers"
val stockPriceDataSchema = StructType(Array(
  StructField("ticker_symbol", StringType, true),
  StructField("stock_day", StringType, true),
  StructField("stock_time", StringType, true),
  StructField("open_p",DoubleType, true),
  StructField("high_p",DoubleType, true),
  StructField("low_p",DoubleType, true),
  StructField("close_p",DoubleType, true),
  StructField("volume_p",IntegerType, true)
))
val avePrice_calculator = udf((a:Double, b:Double, c:Double, d:Double) => (a+b+c+d)/4)
val stockprice_df = sqlContext.read.format("com.databricks.spark.csv").option("header","false").schema(stockPriceDataSchema).load(stock_price_path)
val singleprice_df = stockprice_df.withColumn("avePrice", avePrice_calculator(col("open_p"),col("high_p"),col("low_p"),col("close_p"))).drop("open_p","high_p","low_p","close_p")

val addDateAndTime = udf((date:String, time:String) => date+" "+time)
val stock_final_df = singleprice_df.withColumn("stock_moment", addDateAndTime(col("stock_day"), col("stock_time")).cast("timestamp")).drop("stock_day", "stock_time")
case class Stock_Single_Price_CC(ticker_symbol:String, volume_p:Int, avePrice:Double, stock_moment:java.sql.Timestamp)
val stock_final_ds:Dataset[Stock_Single_Price_CC] = stock_final_df.as[Stock_Single_Price_CC]

//##################################################################################################################################################################
// Broadcast stock_final_ds
//##################################################################################################################################################################
val broadcastVar = sc.broadcast(stock_final_ds)

//##################################################################################################################################################################
// Final Calculation with news_final_ds and stock_final_ds
// Result:
//       (news_day | news_time | ticker | after_15mins | after_1hr | after_2hr | current_price | price_after15mins | price_after1hr | price_after2hr |
// Final Result:
//       (content | header | tickers    | after_15mins | after_1hr | after_2hr | current_price | price_after15mins | price_after1hr | price_after2hr |
//##################################################################################################################################################################











//##################################################################################################################################################################
// Export Result to Files
//##################################################################################################################################################################

// if(fullDetailResult == true){ // In the format of: (title, Alias2TickerRow(alias, ticker))
//   val title_alias_ticker_tuple = news_title_ngrams_and_keywords_only.collect().map(one_set_of_keywords => (one_set_of_keywords, alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias))))
// }else{ // Only contains the matached alias and ticker
//   val title_alias_ticker_tuple = news_title_ngrams_and_keywords_only.collect().map(one_set_of_keywords => alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias)))
// }
//
// if(exportResult == true) {// In order to view result:
//   sc.parallelize(title_alias_ticker_tuple.map(s => s._1.mkString(" ") + " alias-ticker-pair:" +s._2.mkString(" ")).toSeq).saveAsTextFile("title_alias_ticker_temp_result")
//   sc.parallelize(title_alias_ticker_tuple.map(s => s._2.mkString(" ")).toSeq).saveAsTextFile("alias_ticker_only_temp_result")
// }
