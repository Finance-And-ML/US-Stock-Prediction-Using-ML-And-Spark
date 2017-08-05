// import org.apache.spark.sql.SQLContext
// val sqlCtx = new SQLContext(sc)
//
// val artDF = sqlCtx.read.json("/Users/jimmy/Desktop/tmp/2017-07-01.json")

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

import spark.implicits._

val artDF = spark.read.json("/Users/jimmy/Desktop/tmp/2017-07-01.json")

val ts = unix_timestamp($"news_time", "MM/dd/yyyy HH:mm:ss").cast("timestamp")
val art_time = artDF.withColumn("ts", ts)
