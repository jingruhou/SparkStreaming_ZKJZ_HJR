package zkjz.hjr.xjy

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/16.
  */
object distinct {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    // 0 加载数据
    val data = sc.textFile("D:/streamingData/sql/1.txt").cache()
    data.foreach(println)
    // 1 第三列去重后的新RDD
    val column3RDD = data.map(line =>line.split("\t")(3)).map(line =>{
      var list = line.split(",").toList.distinct
      var str = ""
      for (i <- 0 to list.length-1){
        str += list(i)+" "
      }
      str
    })
    // 2 使用zip算子合并两个RDD结果
    val zipResult = data.map(line =>{
      line.split("\t")(1) +" "+ line.split("\t")(2)
    }).zip(column3RDD).map(line => line._1 +","+ line._2)
    zipResult.foreach(println)
    //9829135 2010-4-20 code1 code2 code3
    //9829135 2010-6-26 code1 code2 code3
  }
}
