package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/7.
  */
class zkjz6 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL")
    val sc = new SparkContext(conf)

    //加载文件
    val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/p*")

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
    //println("#######################SUCCESSFUL######################################")
    //处理门诊表的业务逻辑
    var count = 0
    var pcount = 0
    val outclinical = outclinical_diago_rdd.map(_.split("\t").filter(_.length == 4)).map(line =>{
      var strs = line.split("\t")
      pcount=pcount+1
      if(strs.length==4){
        count=count+1
        var l = strs(3)
        var s = strs(0) + "\t" + strs(1) + "\t" + strs(2) + "\t"
        var m = l.length
        while (m >= 1) {
          var j = 0
          while (j < l.length() - m + 1) {
            var s3 = l.substring(j, j + m)
            if (map.contains(s3)) {
              s += map(s3) + "  "
              l = l.replace(s3, "")
            }
            j = j + 1
          }
          m = m - 1
        }
        s
      }

      //println(line)
    })
    println("##########################"+pcount)
    println("###############==4########"+count)
    //写入hdfs
    outclinical
    outclinical.repartition(1).saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/results")
    //关服务
    sc.stop()
  }
}
