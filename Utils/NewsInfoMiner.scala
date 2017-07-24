// :load /home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Utils/NewsInfoMiner.scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset

case class TickerInfoRow(category:String, group:String, sector:String, ticker:String)
case class NewsRow(content:String, keywords:String, news_time:String, news_title:String, sector:String, url:String)
case class Alias2TickerRow(alias:String, ticker:String)

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/20170717.json")
val alias2ticker = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/alias2ticker.json")
val tickerInfo = sqlContext.read.json("/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/meta/tickerInfo.json")

val news_ds: Dataset[NewsRow] = newsdf.filter(line => line(1)!=null).as[NewsRow]

val alias2ticker_ds: Dataset[Alias2TickerRow] = alias2ticker.as[Alias2TickerRow]
val tickerInfo_ds : Dataset[TickerInfoRow] = tickerInfo.as[TickerInfoRow]

val keywords_ds = news_ds.map(s => s.keywords)
val keywords_list = keywords_ds.map(s => s.toLowerCase.replace(","," ").split(" "))

val temp_keywords_entries = keywords_list.collect().map(one_set_of_keywords => (one_set_of_keywords, alias2ticker_ds.collect().filter(line => one_set_of_keywords.contains(line.alias))))
