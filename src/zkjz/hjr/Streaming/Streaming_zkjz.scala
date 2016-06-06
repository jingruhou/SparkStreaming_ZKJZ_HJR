package zkjz.hjr.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2016/5/24.
  */
object Streaming_zkjz {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming_zkjz")
    val ssc = new StreamingContext(conf, Seconds(5.toLong))

    // 1 加载模型 训练数据
    val trainingData = ssc.textFileStream("D://trainingDir").map(Vectors.parse)
    println(trainingData)
    // 2 加载模型 测试数据
    val testData = ssc.textFileStream("D://testDir").map(LabeledPoint.parse)
    println(testData)
    // 3 模型基本参数设置
    val numDimensions = 3
    val numClusters = 2
    // 4 初始化KMeans模型
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)
    // 5 使用模型开始训练数据...
    model.trainOn(trainingData)
    // 6 使用模型预测数据...
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    //启动SparkStreaming计算引擎
    ssc.start()
    ssc.awaitTermination()
  }
}
