package zkjz.hjr.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by Administrator on 2016/5/24.
  */
object StreamingKMeansExample {
  def main(args: Array[String]) {
    //参数设置
    if (args.length != 5) {
      System.err.println(
        "Usage: StreamingKMeansExample " + "<trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>")
      System.exit(1)
    }
    // 0 设置SparkContext
    val conf = new SparkConf().setMaster("local[4]").setAppName("ZKJZ_StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))
    // 1 加载模型训练数据
    //val trainingData = ssc.textFileStream("D://trainingDir").map(Vectors.parse)
    val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)

    // 2 加载模型测试数据
    //val testData = ssc.textFileStream("D://testDir").map(LabeledPoint.parse)
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)
    // 3 KMeans模型初始化
    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)
    // 4 开始训练模型
    model.trainOn(trainingData)
    // 5 预测结果
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    // 6 开启ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
