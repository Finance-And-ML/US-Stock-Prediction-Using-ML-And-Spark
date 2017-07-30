import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

val news_data_path = "/home/lizichen/Desktop/Realtime-Big-Data/US-Stock-Prediction-Using-ML-And-Spark/Sample-Data/article_till_0721/20170*.json"

val sqlContext = new SQLContext(sc)
val newsdf = sqlContext.read.json(news_data_path)

val newsds = newsdf.where("keywords is not null").drop("_corrupt_record", "sector")

newsds.write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("Reuters_withoutSectorTag");
