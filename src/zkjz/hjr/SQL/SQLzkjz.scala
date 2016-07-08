package zkjz.hjr.SQL

import breeze.linalg.diag
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
/**
  * Created by Administrator on 2016/5/30.
  */
object SQLzkjz {

  def main(args:Array[String]): Unit ={
    // 0 初始化配置
    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    val nullData = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/hjr/results/p*")

    val unllcount = nullData.filter(_.contains(" ")).count()
    println("空行数据： "+unllcount)
  }
}
