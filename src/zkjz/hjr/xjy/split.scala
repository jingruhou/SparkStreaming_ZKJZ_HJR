package zkjz.hjr.xjy

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/16.
  */
object split {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("splitResult_zkjz").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:/streamingData/sql/2.txt")
    val codes = data.map(line => {
      var newline = line.split("\t")
      var firstline = newline(0)+"\t"+newline(1)+"\t"
      var codeline = newline(2).split(",")

      var lastline = ""
      for(i <- 0 to codeline.length-1){
          lastline += firstline+codeline(i)+"\n"
      }
      lastline
    })
    codes.foreach(println)
  }
}
