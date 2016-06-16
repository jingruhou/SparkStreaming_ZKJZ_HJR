package zkjz.hjr.xjy

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/14.
  */
object test {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    /**
      *  0 加载原始数据
      */
    val data = sc.textFile("D:/streamingData/sql/1.txt")
    //第三列去重后的新RDD
    val column3 = data.map(line =>line.split("\t")(3)).map(line =>{

      var list = line.split(",").toList.distinct
      var str = ""
      for (i <- 0 to list.length-1){
        str += list(i)+" "
      }
      str
    })
    //合并结果
    val result0 = data.map(line =>{
      line.split("\t")(1) +" "+ line.split("\t")(2)
    }).zip(column3)
    result0.foreach(println)
  }
}
