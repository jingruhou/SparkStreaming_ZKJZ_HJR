package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/25.
  */
object zkjz88 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("zkjz_hjr")
    val sc = new SparkContext(conf)

    //加载文件
    val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/words")

    //将词库表转化为数组
    val counts_word = outclinical_words_rdd.toArray()

    //将转化为数组的词库表放入集合中
    var diag = ""
    var words =""
    var map = Map(diag -> words)//(0)将词库表转化为一个Map（门诊诊断->ICD_CODE）
    for(i <- 0 to counts_word.length-1){
      var line = counts_word(i)
      diag = line.split("\t")(0)
      words = line.split("\t")(1)
      map += (diag -> words)
    }
    //处理门诊表的业务逻辑
    val outclinical = outclinical_diago_rdd.filter(_.split("\001").length == 4).map(line =>{
      /**
        * 处理门诊表的每一行数据
        */
      var strs = line.split("\001")
      var l = strs(3)//(门诊表业务逻辑---1)拿出第四个字段（DIAG_NAME）
      var firstline = strs(0)+"\001"+strs(1)+"\001"+strs(2)+"\001"//（门诊表业务逻辑---2）拿出前三个字段
      var lastline = ""
      var s = line
      var m = l.length//（门诊表业务逻辑--3）拿出第四个字段的长度
      while (m >= 1) {//第四个字段---诊断名不能为空（DIAG_NAME的长度必须>=1）
        var j = 0
        while (j < l.length - m + 1) {
          var s3 = l.substring(j, j + m)
          if (map.contains(s3)) {//去Map里面查找有没有第四个字段
            //s=s.replace(s3,map(s3)+" ")
            lastline += map(s3)+","//检查出Map里面包含了第四个字段（用词库里面的name1,name2,name3...）
            l = l.replace(s3,"")//将第四个字段替换为空
          }
          j = j + 1
        }//一层循环---（先是1234对应找，然后234，对应找；再就是34对应找...）
        m = m - 1
      }//二层循环---(先是123对应找，然后23，对应找；再就是3对应找...)
      firstline+lastline
    })
    /**
      * ReduceByKey过程
      */
    val pairs = outclinical.filter(_.split("\001").length == 4).map(line => {

     // if(line.split("\001").length == 4) {
        val col2 = line.split("\001")
        (col2(1) + "\001" + col2(2), col2(3))
      //}
    })

    //reduceByKey过程举例： val counts = pairs.reduceByKey((a, b) => a + b)
    val result = pairs.reduceByKey((a,b) => a+b).map(line =>{
      line._1 +"\001"+ line._2
    })

    // 1 第三列去重后的新RDD
    val column3RDD = result.map(line =>line.split("\001")(2)).map(line =>{
      var list = line.split(",").toList.distinct
      var str = ""
      for (i <- 0 to list.length-1){
        str += list(i)+","
      }
      str
    })
    // 2 使用zip算子合并两个RDD结果
    val zipResult = result.map(line =>{
      line.split("\001")(0) +"\t"+ line.split("\001")(1)
    }).zip(column3RDD).map(line => line._1 +"\t"+ line._2)
    //zipResult.foreach(println)

    val codes = zipResult.map(line => {
      var newline = line.split("\t")
      var firstline = newline(0)+"\t"+newline(1)+"\t"
      var codeline = newline(2).split(",")

      var lastline = ""
      for(i <- 0 to codeline.length-1){
        lastline += firstline+codeline(i)+"\n"
      }
      lastline
    }).filter(_.split("\n")!=null)

    //写入hdfs
    codes.repartition(1).saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/hjr/results")
    //关服务
    sc.stop()
  }
}
