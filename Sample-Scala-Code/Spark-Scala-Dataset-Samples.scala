// Resources:
// 1. https://www.balabit.com/blog/spark-scala-dataset-tutorial/


// Benefit of using Dataset API:
/*
1.
The Dataset API is available in Spark since 2016 January (Spark version 1.6). It provides an efficient programming interface to deal with structured data in Spark. There are several blogposts about why to use Datasets and what their benefits to RDDs and DataFrames are. Two examples:
“Dataset API combines object-oriented programming style and compile-time type-safety but with the performance benefits of the Catalyst query optimizer. Datasets also use the same efficient off-heap storage mechanism as the DataFrame API.” See more here: http://www.agildata.com/apache-spark-rdd-vs-dataframe-vs-dataset/
*/
/*
2.
“If you want higher degree of type-safety at compile time, want typed JVM objects, take advantage of Catalyst optimization, and benefit from Tungsten’s efficient code generation, use Dataset.” See more here:
https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
*/
