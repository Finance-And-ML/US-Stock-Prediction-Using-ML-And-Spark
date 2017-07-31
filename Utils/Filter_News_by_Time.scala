import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

// Retuers June 7 to July 19
val news_data_path = "/home/lizichen/Desktop/Realtime-Big-Data/News-Article-And-Full-Details-Dataset/Reuters_Cleaned/Reuters_June7_to_July19_noSectorTag.json"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)

// cleaning by news_time
val newsdf_timestamp = newsdf.withColumn("news_datetime", (col("news_time").cast("timestamp"))).drop("news_time")

// select only news from 10:00am to 4:00pm
val getLegitTime = udf((newstime:java.sql.Timestamp) => if(newstime == null) false else if(newstime.getHours() >= 10 && newstime.getHours < 16) true else false)

val news_duringTradingHours = newsdf_timestamp.filter(getLegitTime(col("news_datetime"))).withColumnRenamed("news_datetime", "news_time")

news_duringTradingHours.write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("Reuters_10am_4pm_noSector");
