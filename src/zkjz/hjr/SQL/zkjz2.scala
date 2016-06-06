package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/3.
  */
object zkjz2 {
  def main(args:Array[String]): Unit ={
    // 0 初始化配置
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("spark://10.2.8.11:7077")
    val conf = new SparkConf().setAppName("ZKJZ_SQL")
    val sc = new SparkContext(conf)

    // 2 创建RDD
    // /user/hive/warehouse/test/diag0530
    // /user/hive/warehouse/test/words
   // val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/out/p*")
    val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/door.db/outclinical_words/p*")

    //将数据切分为行数据
    val counts_word = outclinical_words_rdd.toArray()
    //counts_word.foreach(println)
    //k-v
    var diag = ""
    var words =""
    var map = Map(diag -> words)
    //println("#############################"+counts_word.length+"###########################")
    //var k = 0
    for(i <- 0 to counts_word.length-1){
      var line = counts_word(i)
      var line_arr = line.split("\t")
      if(line_arr.length > 1){
        diag = line_arr(0)
      }
      // k=k+1
      // if(k==124384 || k== 124385 || k== 124386){
      println("@@@@@@@@@@@@@@@@@@@@@@@@"+i+line+"@@@@@@@@@@@@@@@@@@@@@@@@")
      //}

      //println("#########################line"+line+"#########################")
      //println("#########################line"+line.length+"#########################")
      //var a = "$"
      //println(a)

      diag = line.split("\t")(0)
      //println("#########################"+diag+"#########################line")
      //println("#########################"+"split0"+diag.length+"#########################")

      if(line.length == 1){

        words="NULL"
        map += (diag -> words)
      }
      if(line.length > 1){
        words = line.split("\t")(1)
        map += (diag -> words)
      }
      //println("#########################"+"split1"+"#########################")
    }
    println("###################SUCCESS#####################")
  }
}
