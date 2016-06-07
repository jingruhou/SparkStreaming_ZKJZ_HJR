package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/7.
  */
object zkjz7 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //加载文件
    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    val outclinical_words_rdd = sc.textFile("D://streamingData//sql//outclinical_words.txt")
    //将词库表转化为数组
    val counts_word = outclinical_words_rdd.toArray()

    //将转化为数组的词库表放入集合中
    var diag = ""
    var words =""
    var map = Map(diag -> words)
    for(i <- 0 to counts_word.length-1){
      var line = counts_word(i)
      diag = line.split("\t")(0)
      words = line.split("\t")(1)
      map += (diag -> words)
    }
    //处理门诊表的业务逻辑
    val outclinical = outclinical_diago_rdd.filter(_.split("\t").length == 4).map(line =>{
      var strs = line.split("\t")
      var l = strs(3)
      var s = line
      var m = l.length
      while (m >= 1) {
        var j = 0
        while (j < l.length - m + 1) {
          var s3 = l.substring(j, j + m)
          if (map.contains(s3)) {
            s=s.replace(s3,map(s3)+"  ")
          }
          j = j + 1
        }
        m = m - 1
      }
      s
    })
    outclinical.foreach(println)
    //ReduceByKey过程
    //R-（2）使用第一MPI_PERSION_ID和第二个VISITOR_DATE字段作为key//rdd1.map(_.split("\t")).filter(_.length==6)
    val pairs = outclinical.map(line => {
      var col2 = line.split("\t")
      (col2(1)+"\t"+col2(2),col2(3))
    })

    pairs.foreach(println)
    //R-（3）reduceByKey过程 val counts = pairs.reduceByKey((a, b) => a + b)
    val result = pairs.reduceByKey((a,b) => a+b).map(line =>{
      line._1 +"\t"+ line._2
    })
    result.foreach(println)
    //写入hdfs
    //result.repartition(1).saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/results")
    //关服务
    sc.stop()
  }
}
