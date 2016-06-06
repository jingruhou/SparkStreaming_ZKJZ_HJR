package zkjz.hjr.ml


import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
/**
  * Created by Administrator on 2016/5/25.
  */
object SummaryStatisticsExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // $example on$
    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )
    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean)  // a dense vector containing the mean value for each column
    println(summary.variance)  // column-wise variance
    println(summary.numNonzeros)  // number of nonzeros in each column
    // $example off$

    sc.stop()
  }
}
