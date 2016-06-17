package zkjz.hjr.xjy

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/16.
  */
object split1 {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("splitResult_zkjz").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:/streamingData/sql/2.txt")
    val codes = data.map(line => {
      var newline = line.split("\t")
      var firstline = newline(0)+"\t"+newline(1)+"\t"
      var codeline = newline(2).split(" ")

      var lastline = ""
      for(i <- 0 to codeline.length-1){
        //lastline += firstline+codeline(i)+"\n"
        lastline += firstline+codeline(i)+"#"
      }
      lastline
    })
    /**
      *  优化结果
      */
   /* val test = sc.parallelize(1 to 3)
    val testResult = test.flatMap(x => x to 9)
    testResult.foreach(println)*/

    val result = codes.flatMap(line =>{
      line.split("#")
    })
    result.foreach(println)
  }
}
