import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

val training_DF = ??

val assembler1 = new VectorAssembler().setInputCols("time_diff").setOutputCol("features")
val assembler2 = new VectorAssembler().setInputCols("price_diff").setOutputCol("label")

//val training = sqlCtx.read.format("libsvm").load("/Users/jimmy/Desktop/tmp/mllib/sample_linear_regression_data.txt")train
val linear = new LinearRegression()
val linearModel = linear.fit(training)

val slope = linearModel.coefficients
