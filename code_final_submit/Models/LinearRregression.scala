import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame


def cal_target(input: DataFrame): coeff ={
  val assembler1 = new VectorAssembler().setInputCols(vector("time_diff")).setOutputCol("features")
  val assembler2 = new VectorAssembler().setInputCols("price_diff").setOutputCol("label")

  val linear = new LinearRegression()
  val linearModel = linear.fit(input)

  return linearModel.coefficients
}





//val training = sqlCtx.read.format("libsvm").load("/Users/jimmy/Desktop/tmp/mllib/sample_linear_regression_data.txt")train
