package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/15.
  */
object test8341 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("zkjz_hjr").setMaster("local")
    val sc = new SparkContext(conf)

    //加载文件
    //val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/p*")
    val outclinical_diago_rdd = sc.textFile("D:/streamingData/sql/outclinical_diago530.txt")
    val outclinical_words_rdd = sc.textFile("D:/streamingData/sql/outclinical_words.txt")

    //将词库表转化为数组
    val counts_word = outclinical_words_rdd.toArray()

    //将转化为数组的词库表放入集合中
    var diag = ""
    var words =""
    var map = Map(diag -> words)
    for(i <- 0 to counts_word.length-1){
      var line = counts_word(i)
      diag = line.split("\001")(0)
      words = line.split("\001")(1)
      map += (diag -> words)
    }
    //处理门诊表的业务逻辑
    val outclinical = outclinical_diago_rdd.filter(_.split("\001").length == 4).map(line =>{

      var strs = line.split("\001")
      var l = strs(3)
      var s = line
      var m = l.length
      while (m >= 1) {
        var j = 0

        while (j < l.length - m + 1) {
          var s3 = l.substring(j, j + m)
          if (map.contains(s3)) {
            s=s.replace(s3,map(s3))
          }
          j = j + 1
        }
        m = m - 1
      }
      s
    })

    //ReduceByKey过程
    val pairs = outclinical.map(line => {
      var col2 = line.split("\001")
      (col2(1)+"\001"+col2(2),col2(3))
    })

    //R-（3）reduceByKey过程 val counts = pairs.reduceByKey((a, b) => a + b)
    val result = pairs.reduceByKey((a,b) => a+b).map(line =>{
      line._1 +"\001"+ line._2
    })
    //写入hdfs
    //result.repartition(1).saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/results")
    result.repartition(1).saveAsTextFile("D:/streamingData/sql")
    //关服务
    sc.stop()
  }
}
