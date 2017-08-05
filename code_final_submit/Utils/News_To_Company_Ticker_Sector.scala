// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

// ticker, date, 		time, 		close, 	high, 	low, 	open, 	volume
// IF,	2017-07-18,	09:32:00,	7.64,	7.64,	7.50,	7.50,	2320

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}


object News_To_Company_Ticker_Sector{

	def main(args: Array[String]){
		val sqlContext = new SQLContext(sc)
		val customSchema = StructType(Array(
		    StructField("ticker", StringType, true),
		    StructField("date", StringType, true),
		    StructField("time", StringType, true),
		    StructField("close", DoubleType, true),
		    StructField("high", DoubleType, true),
		    StructField("low", DoubleType, true),
		    StructField("open", DoubleType, true),
		    StructField("volume", DoubleType, true)))

		val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customSchema).load("/user/lc3397/stock_price/20170703_20170717_1minute/nasdaq_3209_170703_170717/*")
		val selectedData = df.select("ticker", "date", "time", "close")
		val one_day_data = selectedData.filter(selectedData("date") === "2017-07-17")		

		
	}
}


// Warning: export crashes on dumbo!
one_day_data.write.format("com.databricks.spark.csv").option("header", "false").save("20170717_nasdaq_dir")

