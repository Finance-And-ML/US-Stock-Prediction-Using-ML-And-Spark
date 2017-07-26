// spark-shell --packages com.databricks:spark-csv_2.11:1.5.0

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, NGram, StopWordsRemover, VectorAssembler}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}


case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val hasSector = false
val fullDetailResult = false
val exportResult = false

val news_data_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/article_till_0721/2017-07-20-*.json"
val alias2ticker_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/alias2ticker.json"
// tickerInfo.json gives 'sector', 'category' and 'group' attributes for each Ticker
val ticker_info_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/tickerInfo.json"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)
val alias2ticker = sqlContext.read.json(alias2ticker_path)
val tickerInfo = sqlContext.read.json(ticker_info_path)

val alias2ticker_ds: Dataset[Alias2TickerRow] = alias2ticker.as[Alias2TickerRow]

val newsds = newsdf.where("keywords != null")

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
val news_ds_withKeywordsNgramList = news_ds.map(s => (s.content, s.news_time.split(" ")(0), s.news_time.split(" ")(1), s.news_title, s.url, s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4 ++ s.keywords.split(",")))
// To rename column names
val newColumnNames = Seq("content", "news_date", "news_minute", "news_title", "url", "ngramKeywords")
val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF(newColumnNames: _*)
case class News_NGrams_CC(content:String, news_date:String, news_minute:String, news_title:String, url:String, ngramKeywords:Array[String])
val new_ds_withNgramWordsArray:Dataset[News_NGrams_CC] = new_df_withNgramWordsArray.as[News_NGrams_CC]
// Proceed the matching - result is in the type of Array[(String, String, String, String, Array[String])]
val result_array_withMatchedTickers = new_ds_withNgramWordsArray.collect().map(s => (s.news_title, s.news_date, s.news_minute, s.content, s.url, alias2ticker_ds.collect().withFilter(line => s.ngramKeywords.contains(line.alias)).map(line => line.ticker)))

// create rdd from the result
// val news_rdd_withMatchedTickersArray = sc.parallelize(result_array_withMatchedTickers.toSeq)

// create df/ds from the result
val news_df_withTickersArray = result_array_withMatchedTickers.toSeq.toDF("content", "news_day", "news_time", "news_title", "url", "tickers")
case class News_withTickers_CC(content:String, news_day:String, news_time:String, news_title:String, url:String, tickers:Array[String])
val news_ds_withTickersArray:Dataset[News_withTickers_CC] = news_df_withTickersArray.as[News_withTickers_CC]
//news_time: 2017-07-19 22:26:00

// here stock price comes in
val stock_price_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Intraday-Dataset/Intraday-Data/20170718_20170721_1minute/alltickers/*.csv"
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
val stockprice_df = sqlContext.read.format("com.databricks.spark.csv").option("header","false").schema(stockPriceDataSchema).load(stock_price_path)
val singleprice_df = stockprice_df.withColumn("avePrice", myudf(col("open_p"),col("high_p"),col("low_p"),col("close_p"))).drop("open_p","high_p","low_p","close_p")
case class Stock_Single_Price_CC(ticker_symbol:String, stock_day:String, stock_time:String, volume_p:Int, avePrice:Double)
val singleprice_ds:Dataset[Stock_Single_Price_CC] = singleprice_df.as[Stock_Single_Price_CC]


val result = news_ds_withTickersArray.collect().map(news => singleprice_ds.collect().filter(stock => news.tickers.contains(stock.ticker_symbol) && stock.stock_day == news.news_day && stock.stock_time == news.news_time))

// Now work on the news_ds_withTickersArray and singlePrice_ds to get the result:
// news_day   == stock_day
// news_time  == stock_time
// tickers -> ticker_1 = ticker_symbol_1
//         -> ticker_2 = ticker_symbol_2
// Result:
//       (news_day | news_time | ticker | moment_price | 15min_price | 1hr_price | 2hr_price | 6hr_price)







/*

if(fullDetailResult == true){ // In the format of: (title, Alias2TickerRow(alias, ticker))
  val title_alias_ticker_tuple = news_title_ngrams_and_keywords_only.collect().map(one_set_of_keywords => (one_set_of_keywords, alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias))))
}else{ // Only contains the matached alias and ticker
  val title_alias_ticker_tuple = news_title_ngrams_and_keywords_only.collect().map(one_set_of_keywords => alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias)))
}

if(exportResult == true) {// In order to view result:
  sc.parallelize(title_alias_ticker_tuple.map(s => s._1.mkString(" ") + " alias-ticker-pair:" +s._2.mkString(" ")).toSeq).saveAsTextFile("title_alias_ticker_temp_result")
  sc.parallelize(title_alias_ticker_tuple.map(s => s._2.mkString(" ")).toSeq).saveAsTextFile("alias_ticker_only_temp_result")
}

*/
