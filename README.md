# Stock Price Prediction via Financial News Sentiment Analysis

### About:


### Approach:


### Data Source:
1. WSJ News 
 - Full-attribute Wall St. Journal News, including news published timestamp, keywords list, news headline, news body content, and news url. All in JSON file format.
2. Reuters News 
 - Same as WSJ news data.
3. Intra-Day Stock Price 
 - By-minute stock price scrapped from Google Stock API.
 - Use tickers from three major U.S. Enquity Stock Trade Exchanges: Nasdaq, NYSE, AMEX.
 - Scrapped data only contains the records that have price changes, meaning that if one stock does not have any price change in one hour, there will be no by-minute record within that hour. 
4. meta/alias2Ticker.json
 - This file is collected from running queries with stock tickers to CityFalcon (https://www.cityfalcon.com/). It is still under construction and cleaning for better matching accuracy.
 - JSON schema: {"alias":"american airlines group","ticker":"AAL"}
 - use stopword.txt to delete tickers and aliases that show as common English words.
5. meta/tickerInfo.json
 - JSON schema: {"ticker": "ABC", "sector": "Some_Sector", "category": "Some_Cat", "group": "Some_Group"}
 - Includes 9 sectors, 31 categories, 212 groups


##### Reference:
1. Git: http://www.vogella.com/tutorials/Git/article.html
2. UDFs: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-udfs.html
3. Dataset: https://www.balabit.com/blog/spark-scala-dataset-tutorial/
4. DataFrames: https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html  
5. Word2Vec Sentiments: https://github.com/linanqiu/word2vec-sentiments


### To-Do:
#### Improvement:
1. Stocks that do not have contineuous by-minute price record needs better simulation. Currently, the simulation is done by having all the missing records be the one that appear the latest. We need to mock up the missing price in a linear fashion between two price record.
2. Inspect the alias2ticker json file: 
 - To eliminate confusing aliases.  
 - To remove aliases that referring to non-US traded ticker symbols.   
 - To add potential match-able aliases.  
 - To add ticker symbols that constains .[dot]  
 - To add alias-ticker pairs for missing tickers.
3. We have not tried the Deep Learning approaches; however, past researches have shown steep improvement with DL.
4. Automate the data ETL process.
5. Render real-time result on Tableau.


#### Experiment:
1. Currently, the target is computed from the average price of the _open_, _close_, _highest_, and _lowest_. We have not take the _volume_ into the training. We should also try to work out variations on target value.
2. Consult with Financial Engineering researchers on constructing models that have more factors in consideration. 
