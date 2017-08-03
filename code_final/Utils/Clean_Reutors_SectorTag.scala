import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

val news_data_path = "/home/lizichen/Desktop/Realtime-Big-Data/News-Article-And-Full-Details-Dataset/tempReuter/*.json"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)

// drop _corrupt_record and sector
val newsds = newsdf.where("keywords is not null").drop("_corrupt_record", "sector")

// convert news_time to java.sql.Timestamp
val newsdf_timestamp = newsds.withColumn("news_datetime", (col("news_time").cast("timestamp"))).drop("news_time")

// Define UDF to filter for time that is between [10:00am and 4:00pm]
val getLegitTime = udf((newstime:java.sql.Timestamp) => if(newstime == null) false else if(newstime.getHours() >= 10 && newstime.getHours < 16) true else false)

// Filter out news after trading hours and rename news_datetime back to news_time
val news_duringTradingHours = newsdf_timestamp.filter(getLegitTime(col("news_datetime"))).withColumnRenamed("news_datetime", "news_time")

// Write new JSON result file to disk
news_duringTradingHours.write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("News_10am_4pm_noSector");
