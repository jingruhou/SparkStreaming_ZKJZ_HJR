package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/7.
  */
object test2 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//b")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/p*")
    val count = outclinical_diago_rdd.count()
    println("词库表的行数： "+count)
    val rdd3 = outclinical_diago_rdd.map(_.split("|$|").filter(_.length==4))
    val filterCount = rdd3.count()
    println("过滤后的词库表的行数： "+filterCount)
  }
}
