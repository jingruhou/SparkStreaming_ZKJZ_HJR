package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/28.
  */
object text88 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zkjz_hjr").setMaster("local")
    val sc = new SparkContext(conf)

    //加载文件
    val outclinical_diago_rdd = sc.textFile("a")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/words")


    //outclinical_diago_rdd.foreach(println)
    outclinical_diago_rdd.filter(_.split(",").length ==3).foreach(println)
  }

}
