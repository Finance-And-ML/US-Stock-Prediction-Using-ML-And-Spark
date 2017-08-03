//Local: spark-shell --driver-memory 3G --executor-memory 5G --executor-cores 3
//Dumbo: spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8
//for Scala 2.11 spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
//for Scala 2.10 (Dumbo) $SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
//--conf spark.kryoserializer.buffer.max=512

import org.apache.spark.sql.{SQLContext, Dataset, DataFrame}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, NGram, StopWordsRemover, VectorAssembler, StandardScaler, PCA}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val news_data_path = "/user/cyy292/project/test"
val alias2ticker_path = "/user/cyy292/project/alias2ticker.json"
val ticker_info_path = "/user/cyy292/project/tickerInfo.json" // tickerInfo.json gives 'sector', 'category' and 'group' attributes for each Ticker
val stock_price_path = "/user/cyy292/project/alltickers"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)
val alias2ticker = sqlContext.read.json(alias2ticker_path)
val tickerInfo = sqlContext.read.json(ticker_info_path)

val alias2ticker_ds: Dataset[Alias2TickerRow] = alias2ticker.as[Alias2TickerRow]
val tickerInfo_ds: Dataset[TickerInfoRow] = tickerInfo.as[TickerInfoRow]
//broadcast
val alias2ticker_ds_bc = sc.broadcast(alias2ticker_ds)
val tickerInfo_ds_bc = sc.broadcast(tickerInfo_ds)

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

case class News_title_tk_cleaned_ngram_CC(content:String, keywords:String, news_time:String, news_title:String,  url:String, news_title_tk:Array[String], news_title_clean:Array[String], news_title_ngrams_2:Array[String], news_title_ngrams_3:Array[String], news_title_ngrams_4:Array[String])
val news_ds: Dataset[News_title_tk_cleaned_ngram_CC] = ngram_4_trans.as[News_title_tk_cleaned_ngram_CC]
val news_ds_withKeywordsNgramList = news_ds.map(s => (s.content, s.news_time, s.news_title, s.url, s.news_title_clean ++ s.news_title_ngrams_2 ++ s.news_title_ngrams_3 ++ s.news_title_ngrams_4 ++ s.keywords.split(",")))
// To rename column names
val newColumnNames = Seq("content", "news_time", "news_title", "url", "ngramKeywords")

// This line does not work for Dumbo
val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF.withColumnRenamed("_1","content").withColumnRenamed("_2","news_time").withColumnRenamed("_3","news_title").withColumnRenamed("_4","url").withColumnRenamed("_5","ngramKeywords")
// This works in Dumbo:
// val new_df_withNgramWordsArray = news_ds_withKeywordsNgramList.toDF.withColumnRenamed("_1","content").withColumnRenamed("_2","news_date").withColumnRenamed("_3","news_minute").withColumnRenamed("_4","news_title").withColumnRenamed("_5","url").withColumnRenamed("_6","ngramKeywords")

case class News_NGrams_CC(news_time:String, news_title:String, url:String, content:String, ngramKeywords:Array[String])
val new_ds_withNgramWordsArray:Dataset[News_NGrams_CC] = new_df_withNgramWordsArray.as[News_NGrams_CC]
// Proceed the matching - result is in the type of Array[(String, String, String, String, Array[String])]
val result_array_withMatchedTickers_all = new_ds_withNgramWordsArray.collect().map(s => (s.news_title, s.news_time, s.url, s.content, alias2ticker_ds_bc.value.collect().withFilter(line => s.ngramKeywords.contains(line.alias)).map(line => line.ticker)))

val result_array_withMatchedTickers = result_array_withMatchedTickers_all.filter(news => news._5.length == 1).map(news => (news._1, news._2, news._3, news._4, news._5(0)))

// create df/ds from the result
val news_df_withTickersArray = result_array_withMatchedTickers.toSeq.toDF("news_title", "news_time", "url", "content", "tickers")
//news_df_withTickersArray.cache
val news_df_withTickersArray_timestampDate = news_df_withTickersArray.withColumn("news_datetime", (col("news_time").cast("timestamp"))).drop("news_time")

val add7 = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 7*60*1000))
val add15 = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 15*60*1000))
val add30 = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 30*60*1000))
val add60 = udf((currentTime:java.sql.Timestamp) => new java.sql.Timestamp(currentTime.getTime + 60*60*1000))

val news_final_df = news_df_withTickersArray_timestampDate.withColumn("after_7m", add7(col("news_datetime"))).withColumn("after_15m", add15(col("news_datetime"))).withColumn("after_30m", add30(col("news_datetime"))).withColumn("after_60m", add60(col("news_datetime")))


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
// Final Calculation with news_final_ds and stock_final_ds
//##################################################################################################################################################################

val join_table = news_final_df.join(stock_final_df, news_final_df("tickers") === stock_final_df("ticker_symbol"))
join_table.cache
val price_current_df = join_table.where("news_datetime = stock_moment").withColumnRenamed("volume_p", "volume_cur").withColumnRenamed("avePrice", "price_cur").withColumn("id_cur",monotonicallyIncreasingId)
val price_7m_df = join_table.where("after_7m = stock_moment").withColumnRenamed("volume_p", "volume_7m").withColumnRenamed("avePrice", "price_7m").withColumn("id_7m",monotonicallyIncreasingId).select($"id_7m", $"volume_7m", $"price_7m")
val price_15m_df = join_table.where("after_15m = stock_moment").withColumnRenamed("volume_p", "volume_15m").withColumnRenamed("avePrice", "price_15m").withColumn("id_15m",monotonicallyIncreasingId).select($"id_15m", $"volume_15m", $"price_15m")
val price_30m_df = join_table.where("after_30m = stock_moment").withColumnRenamed("volume_p", "volume_30m").withColumnRenamed("avePrice", "price_30m").withColumn("id_30m",monotonicallyIncreasingId).select($"id_30m", $"volume_30m", $"price_30m")
val price_60m_df = join_table.where("after_60m = stock_moment").withColumnRenamed("volume_p", "volume_60m").withColumnRenamed("avePrice", "price_60m").withColumn("id_60m",monotonicallyIncreasingId).select($"id_60m", $"volume_60m", $"price_60m")

val price_cur_7m_df = price_current_df.join(price_7m_df, price_current_df("id_cur") === price_7m_df("id_7m"))
val target_7m = price_cur_7m_df.select(($"*"), (($"price_7m") - ($"price_cur"))/($"price_cur") as "price_7m_diff")
val price_cur_15m_df = target_7m.join(price_15m_df, target_7m("id_cur") === price_15m_df("id_15m"))
val target_15m = price_cur_15m_df.select(($"*"), (($"price_15m") - ($"price_cur"))/($"price_cur") as "price_15m_diff")
val price_cur_30m_df = target_15m.join(price_30m_df, target_15m("id_cur") === price_30m_df("id_30m"))
val target_30m = price_cur_30m_df.select(($"*"), (($"price_30m") - ($"price_cur"))/($"price_cur") as "price_30m_diff")
val price_cur_60m_df = target_30m.join(price_60m_df, target_30m("id_cur") === price_60m_df("id_60m"))
val target_60m = price_cur_60m_df.select(($"*"), (($"price_60m") - ($"price_cur"))/($"price_cur") as "price_60m_diff")

//##################################################################################################################################################################
// match tickerInfo
//##################################################################################################################################################################

val target_tickerInfo_df = target_60m.join(tickerInfo, target_60m("tickers") === tickerInfo("ticker"))
case class target_tickerInfo_CC(content: String, tickers: String, sector: String, category: String, price_7m_diff: Double, price_15m_diff: Double, price_30m_diff: Double, price_60m_diff: Double, news_datetime: java.sql.Timestamp)
val target_tickerInfo_ds:Dataset[target_tickerInfo_CC] = target_tickerInfo_df.select("content", "tickers", "sector", "category", "price_7m_diff", "price_15m_diff", "price_30m_diff", "price_60m_diff", "news_datetime").as[target_tickerInfo_CC]


//##################################################################################################################################################################
// label define
//##################################################################################################################################################################

def get_label(price_diff: Double): Double = {
  if (price_diff >= 0.01) {return 3.0}
  else if (price_diff >= -0.01 && price_diff < 0.01) {return 2.0}
  else {return 1.0}
}

def get_sector(sector: String): Vector = sector match {
  case "Basic Materials" => Vectors.sparse(10, Array(0), Array(1.0))
  case "Conglomerates" => Vectors.sparse(10, Array(1), Array(1.0))
  case "Consumer Goods" => Vectors.sparse(10, Array(2), Array(1.0))
  case "Financial" => Vectors.sparse(10, Array(3), Array(1.0))
  case "Healthcare" => Vectors.sparse(10, Array(4), Array(1.0))
  case "Industrial Goods" => Vectors.sparse(10, Array(5), Array(1.0))
  case "Services" => Vectors.sparse(10, Array(6), Array(1.0))
  case "Technology" => Vectors.sparse(10, Array(7), Array(1.0))
  case "Utilities" => Vectors.sparse(10, Array(8), Array(1.0))
  case _ => Vectors.sparse(10, Array(9), Array(1.0))
}

def get_category(sector: String): Vector = sector match {
  case "Aerospace/Defense" => Vectors.sparse(32, Array(0), Array(1.0))
  case "Automotive" => Vectors.sparse(32, Array(1), Array(1.0))
  case "Banking" => Vectors.sparse(32, Array(2), Array(1.0))
  case "Chemicals" => Vectors.sparse(32, Array(3), Array(1.0))
  case "Computer Hardware" => Vectors.sparse(32, Array(4), Array(1.0))
  case "Computer Software & Services" => Vectors.sparse(32, Array(5), Array(1.0))
  case "Conglomerates" => Vectors.sparse(32, Array(6), Array(1.0))
  case "Consumer Durables" => Vectors.sparse(32, Array(7), Array(1.0))
  case "Consumer NonDurables" => Vectors.sparse(32, Array(8), Array(1.0))
  case "Diversified Services" => Vectors.sparse(32, Array(9), Array(1.0))
  case "Drugs" => Vectors.sparse(32, Array(10), Array(1.0))
  case "Electronics" => Vectors.sparse(32, Array(11), Array(1.0))
  case "Energy" => Vectors.sparse(32, Array(12), Array(1.0))
  case "Financial Services" => Vectors.sparse(32, Array(13), Array(1.0))
  case "Food & Beverage" => Vectors.sparse(32, Array(14), Array(1.0))
  case "Health Services" => Vectors.sparse(32, Array(15), Array(1.0))
  case "Insurance" => Vectors.sparse(32, Array(16), Array(1.0))
  case "Internet" => Vectors.sparse(32, Array(17), Array(1.0))
  case "Leisure" => Vectors.sparse(32, Array(18), Array(1.0))
  case "Manufacturing" => Vectors.sparse(32, Array(19), Array(1.0))
  case "Materials & Construction" => Vectors.sparse(32, Array(20), Array(1.0))
  case "Media" => Vectors.sparse(32, Array(21), Array(1.0))
  case "Metals & Mining" => Vectors.sparse(32, Array(22), Array(1.0))
  case "Real Estate" => Vectors.sparse(32, Array(23), Array(1.0))
  case "Retail" => Vectors.sparse(32, Array(24), Array(1.0))
  case "Specialty Retail" => Vectors.sparse(32, Array(25), Array(1.0))
  case "Telecommunications" => Vectors.sparse(32, Array(26), Array(1.0))
  case "Tobacco" => Vectors.sparse(32, Array(27), Array(1.0))
  case "Transportation" => Vectors.sparse(32, Array(28), Array(1.0))
  case "Utilities" => Vectors.sparse(32, Array(29), Array(1.0))
  case "Wholesale" => Vectors.sparse(32, Array(30), Array(1.0))
  case _ => Vectors.sparse(32, Array(31), Array(1.0))
}

val target_labeled_ds = target_tickerInfo_ds.map(row => (row.content, row.tickers, row.sector, row.category, get_sector(row.sector).toArray, get_category(row.category).toArray, get_label(row.price_7m_diff), get_label(row.price_15m_diff), get_label(row.price_30m_diff), get_label(row.price_60m_diff), row.news_datetime))
val target_labeled_df = target_labeled_ds.toDF().withColumnRenamed("_1", "content").withColumnRenamed("_2", "tickers").withColumnRenamed("_3", "sector").withColumnRenamed("_4", "category").withColumnRenamed("_5", "sector_features").withColumnRenamed("_6", "category_features").withColumnRenamed("_7", "label_7m").withColumnRenamed("_8", "label_15m").withColumnRenamed("_9", "label_30m").withColumnRenamed("_10", "label_60m").withColumnRenamed("_11", "news_datetime")

target_labeled_df.write.mode("Overwrite").json("/user/cyy292/project/target_labeled.json")

target_labeled_df.toJSON.coalesce(1,true).saveAsTextFile("")
//selected data distribution
//
// var a = 0
// var start = 0
// for (a <- 1 to 10){
//   val len = arraySorted.size/10
//   val mid = start + len/2
//   println(arraySorted(mid))
//   start = start + len
}
