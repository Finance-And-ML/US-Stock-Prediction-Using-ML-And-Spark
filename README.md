# Stock Price Prediction via Financial News Sentiment Analysis

##### spark-shell enviroment configuration
- spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8 --packages com.databricks:spark-csv_2.10:1.5.0

##### Databrick read csv files:
//for Scala 2.11 spark-shell --packages com.databricks:spark-csv_2.11:1.5.0  
//for Scala 2.10 (Dumbo) $SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.10:1.5.0  

##### meta
- tickerinfo.json schema:
> {"ticker": "SOME_TICKER", "sector": "Market", "category": "Electronic", "group": "Some_Group"}
> 9 sectors, 31 categories, 212 groups

- alias2ticker.json schema:
> {"alias":"american airlines group","ticker":"AAL"}
> use stopword.txt to delete ticker with common use

##### Resources:
1. Git: http://www.vogella.com/tutorials/Git/article.html


##### Resources:
1. Git: http://www.vogella.com/tutorials/Git/article.html


##### Resources:
1. Git: http://www.vogella.com/tutorials/Git/article.html
2. UDFs: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-udfs.html
3. Dataset: https://www.balabit.com/blog/spark-scala-dataset-tutorial/
4. DataFrames: https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html  
5. Word2Vec Sentiments: https://github.com/linanqiu/word2vec-sentiments

