package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/6.
  */
object test1 {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("spark://10.2.8.11:7077")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL")
    val sc = new SparkContext(conf)

    // 2 创建RDD
    // /user/hive/warehouse/test/diag0530
    // /user/hive/warehouse/test/words
    //val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/out/p*")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/door.db/outclinical_words/p*")

    //val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/p*")

    // val outclinical_diago_rdd = sc.textFile("hdfs://192.168.13.130:8020/user/houjr/sql/outclinical_diago530.txt")
    //val outclinical_words_rdd = sc.textFile("hdfs://192.168.13.130:8020/user/houjr/sql/outclinical_words.txt")

    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt").map(line =>{
      //println("################# map() ###############")
      line.length
    })
    outclinical_diago_rdd.foreach(println)
  }

}
