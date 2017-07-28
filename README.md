# Stock Price Prediction via Financial News Sentiment Analysis

#### spark-shell enviroment configuration
- spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8 --packages com.databricks:spark-csv_2.10:1.5.0


##### meta
- tickerinfo.json schema:
> {"ticker": "SOME_TICKER", "sector": "Market", "category": "Electronic", "group": "Some_Group"}

- alias2ticker.json schema:
> {"alias":"american airlines group","ticker":"AAL"}
