package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/1.
  */
object test extends App{
    // 0 初始化配置
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("spark://10.2.8.11:7077")
    val conf = new SparkConf().setAppName("ZKJZ_SQL")
    val sc = new SparkContext(conf)
    // 2 创建RDD
    // /user/hive/warehouse/test/diag0530
    // /user/hive/warehouse/test/words
    val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/diag0530")
    val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/words")

    //val outclinical_diago_rdd = sc.textFile("hdfs://10.2.1.111:8020/user/hive/warehouse/diag.db/outclinical_diag0530/")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.1.111:8020/user/hive/warehouse/diag.db/outclinical_words/")

    //val outclinical_diago_rdd = sc.textFile("hdfs://192.168.13.130:8020/user/houjr/sql/outclinical_diago530.txt")
    //val outclinical_words_rdd = sc.textFile("hdfs://192.168.13.130:8020/user/houjr/sql/outclinical_words.txt")

    //val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    //val outclinical_words_rdd = sc.textFile("D://streamingData//sql//outclinical_words.txt")

    val counts_word = outclinical_words_rdd.flatMap(line => line.split("\n")).toArray()
    //k-v
    var diag = ""
    var words =""
    var map = Map(diag -> words)

    for(i <- 0 to counts_word.length-1){
      var line = counts_word(i)

      diag = line.split("\t")(0)
      words = line.split("\t")(1)
      map += (diag -> words)
    }
    //将outclinical_diago_rdd数据集转化为数组
    var counts = outclinical_diago_rdd.flatMap(line => line.split("\n")).toArray()

    for(i <- 0 to counts.length-1){
      var line = counts(i)
      var l = line.split("\t")(3)
      var s = line.split("\t")(0)+"\t"+line.split("\t")(1)+"\t"+line.split("\t")(2)+"\t"
      var m = l.length
      while ( m >= 1){
        // println(m)
        var j=0
        while(j<l.length()-m+1){
          var s3 = l.substring(j,j + m)
          //println(s3)
          if(map.contains(s3)){
            //println(map(s3))
            s += map(s3)+"."
            l=l.replace(s3,"")
          }
          j=j+1
        }
        m=m-1
      }
      //写出
      //println(s)
      val result = sc.parallelize(s).cache()
      //result.foreach(println)
      //result.repartition(1).saveAsTextFile("D://streamingData//sql//test")+System.currentTimeMillis()
      //result.repartition(1).saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/test/results/result"+ System.currentTimeMillis())
      //result.repartition(1).saveAsTextFile("hdfs://192.168.13.130:8020/user/houjr/sql/results/result"+ System.currentTimeMillis())
      val dfsFilename = "hdfs://10.2.8.11:8020/user/hive/warehouse/test/results/result" + "/dfs_read_write_test"
      val fileRDD = sc.parallelize(s)
      fileRDD.saveAsTextFile(dfsFilename)
    }
}
