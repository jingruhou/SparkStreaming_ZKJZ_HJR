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
  /*  1	9829135	2010-4-20	code1,code1,code2,code3
      2	9829135	2010-6-26	code1,code2,code2,code3
      3	9829135	2011-8-20	code1,code2,code3,code3
      4	9829135	2011-8-21	code4,code4,code5,code6
      5	9829135	2011-9-13	code4,code5,code5,code6
      6	9829135	2011-9-22	code4,code5,code6,code6
      7	9829135	2011-10-31	code7,code7,code8,code9
      8	9829135	2011-11-3	code7,code8,code8,code9
      9	9829135	2011-11-11	code7,code8,code9,code9
      10	9829135	2011-12-18	code10,code10,code11,code12*/

    val data = sc.textFile("D:/streamingData/sql/1.txt")
    data.foreach(println)

    //取出第三列数据
    val column3 = data.map(line => {
      line.split("\t")(3)
    })
    column3.foreach(println)
    /*code1,code1,code2,code3
      code1,code2,code2,code3
      code1,code2,code3,code3
      code4,code4,code5,code6
      code4,code5,code5,code6
      code4,code5,code6,code6
      code7,code7,code8,code9
      code7,code8,code8,code9
      code7,code8,code9,code9
      code10,code10,code11,code12*/
    //使用逗号分隔符splite，然后使用RDD的 Distinct算子去重
    val result1 = column3.map(line =>line.split(","))
    result1.foreach(println)

    for(i <- 0 to column3.count().toInt){
      //val col_line =
    }
//    scala> data.flatMap(line => line.split("\\s+")).collect
//    res61: Array[String] = Array(hello, world, hello, spark, hello, hive, hi, spark)
//
//    scala> data.flatMap(line => line.split("\\s+")).distinct.collect
//    res62: Array[String] = Array(hive, hello, world, spark, hi)
  }
}
