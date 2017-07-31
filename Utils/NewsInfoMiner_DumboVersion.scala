//Local: spark-shell --driver-memory 3G --executor-memory 5G --executor-cores 3
//Dumbo: spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8
//for Scala 2.11 spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
//for Scala 2.10 (Dumbo) $SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, NGram, StopWordsRemover, VectorAssembler}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}

case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val news_data_path = "/user/cyy292/project/test"
val alias2ticker_path = "/user/cyy292/project/alias2ticker.json"
val ticker_info_path = "/user/cyy292/project/tickerInfo.json" // tickerInfo.json gives 'sector', 'category' and 'group' attributes for each Ticker
val stock_price_path = "/user/cyy292/project/alltickers"

val sqlContext = new SQLContext(sc)
import sqlContext._
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

// This line does not work for Dumbo
val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF.withColumnRenamed("_1","content").withColumnRenamed("_2","news_time").withColumnRenamed("_3","news_title").withColumnRenamed("_4","url").withColumnRenamed("_5","ngramKeywords")
// This works in Dumbo:
// val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF.withColumnRenamed("_1","content").withColumnRenamed("_2","news_date").withColumnRenamed("_3","news_minute").withColumnRenamed("_4","news_title").withColumnRenamed("_5","url").withColumnRenamed("_6","ngramKeywords")

case class News_NGrams_CC(content:String, news_time:String, news_title:String, url:String, ngramKeywords:Array[String])
val new_ds_withNgramWordsArray:Dataset[News_NGrams_CC] = new_df_withNgramWordsArray.as[News_NGrams_CC]
// Proceed the matching - result is in the type of Array[(String, String, String, String, Array[String])]
val result_array_withMatchedTickers_all = new_ds_withNgramWordsArray.collect().map(s => (s.news_title, s.news_time, s.url, alias2ticker_ds.collect().withFilter(line => s.ngramKeywords.contains(line.alias)).map(line => line.ticker)))

val result_array_withMatchedTickers = result_array_withMatchedTickers_all.filter(news => news._4.length == 1).map(news => (news._1, news._2, news._3, news._4(0)))

// create df/ds from the result
val news_df_withTickersArray = result_array_withMatchedTickers.toSeq.toDF("news_title", "news_time", "url", "tickers")
//news_df_withTickersArray.cache
val news_df_withTickersArray_timestampDate = news_df_withTickersArray.withColumn("news_datetime", (col("news_time").cast("timestamp"))).drop("news_time")

val add15mins = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 15*60*1000))
val add1hr    = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 60*60*1000))
val add2hr    = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 120*60*1000))

val news_final_df = news_df_withTickersArray_timestampDate.withColumn("after_15mins", add15mins(col("news_datetime"))).withColumn("after_1hr", add1hr(col("news_datetime"))).withColumn("after_2hr", add2hr(col("news_datetime")))

case class News_withTickers_CC(news_title:String, url:String, tickers:String, news_datetime:java.sql.Timestamp, after_15mins:java.sql.Timestamp, after_1hr:java.sql.Timestamp, after_2hr:java.sql.Timestamp)
val news_final_ds:Dataset[News_withTickers_CC] = news_final_df.as[News_withTickers_CC]


//##################################################################################################################################################################
// here stock price comes in
//##################################################################################################################################################################

// val stock_price_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Intraday-Dataset/Intraday-Data/20170718_20170721_1minute/alltickers/*.csv"
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
// does not work for dumbo:
val singleprice_df = stockprice_df.withColumn("avePrice", avePrice_calculator(col("open_p"),col("high_p"),col("low_p"),col("close_p"))).drop("open_p").drop("high_p").drop("low_p").drop("close_p")
// have to seperate the drop in Do:
//

val addDateAndTime = udf((date:String, time:String) => date+" "+time)
val stock_final_df = singleprice_df.withColumn("stock_moment", addDateAndTime(col("stock_day"), col("stock_time")).cast("timestamp")).drop("stock_day").drop("stock_time")
case class Stock_Single_Price_CC(ticker_symbol:String, volume_p:Int, avePrice:Double, stock_moment:java.sql.Timestamp)
val stock_final_ds:Dataset[Stock_Single_Price_CC] = stock_final_df.as[Stock_Single_Price_CC]



//##################################################################################################################################################################
// Broadcast stock_final_ds
//##################################################################################################################################################################
val broadcastVar = sc.broadcast(stock_final_ds)

//##################################################################################################################################################################
// Final Calculation with news_final_ds and stock_final_ds
//##################################################################################################################################################################

// val addTime_udf = udf((a:String) => addTime(a))

// val result = news_final_ds.take(10).map(news => broadcastVar.value.collect().filter(stock => news.tickers.contains(stock.ticker_symbol) && stock.stock_moment == news.news_datetime))

val join_table = news_final_df.join(stock_final_df, news_final_df("tickers") === stock_final_df("ticker_symbol"))

val result = join_table.where("news_datetime = stock_moment")

news_final_df.registerTempTable("T1")
stock_final_df.registerTempTable("T2")
val test = sqlContext.sql("""SELECT T1.tickers WHERE T1.tickers = T2.ticker_symbol""")

val joinedDF = news_final_df.as('a).join(stock_final_df.as('b), $"a.tickers" === $"b.ticker_symbol" AND $"a.news_datetime" === $"b.stock_moment")
//
// val takeAll = false
//
// if (takeAll == true){
//   val result = news_ds_withTickersArray.collect().map(news => singleprice_ds.collect().filter(stock => news.tickers.contains(stock.ticker_symbol) && stock.stock_day == news.news_day && stock.stock_time == news.news_time))
// }else{// for testing only
//   val result = news_ds_withTickersArray.take(10).map(
//     news => singleprice_ds.collect().withFilter(
//       stock => (news.tickers.contains(stock.ticker_symbol)) && (stock.stock_day == news.news_day) && (stock.stock_time == news.news_time) || (stock.stock_time == udf(news.news_time))
//     ).map(s => s.avePrice)
//   )
// }
//
// val result = news_ds_withTickersArray.take(10).map(
//   news => stock_final_ds.collect().withFilter(
//     stock => (news.tickers.contains(stock.ticker_symbol)) && (stock.stock_day == news.news_day) && (stock.stock_time == news.news_time) || (stock.stock_time == "09:30:00") || (stock.stock_time == "10:30:00"))
//   )
// )

// Now work on the news_ds_withTickersArray and singlePrice_ds to get the result:
// news_day   == stock_day
// news_time  == stock_time
// tickers -> ticker_1 = ticker_symbol_1
//         -> ticker_2 = ticker_symbol_2
// Result:
//       (news_day | news_time | ticker | moment_price | 15min_price | 1hr_price | 2hr_price)
// Final Result:
//       (content | header | tickers    | k1           | k2          | k3        | k4




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
