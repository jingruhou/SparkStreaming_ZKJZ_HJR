package zkjz.hjr.SQL

import breeze.linalg.split
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/6.
  */
object test1 {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL")//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val outclinical_diago_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/word/p*")
    //val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    //val outclinical_words_rdd = sc.textFile("hdfs://10.2.8.11:8020/user/hive/warehouse/words/p*")
    //val rdd1 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==4)
      //  println("##############################==4##############"+rdd1.count())
//    val rdd2 = outclinical_diago_rdd.map(line =>{
//      var newline = line.split("\001")
//      if(newline.length!=4){
//        println("#########################"+"\t"+newline)
//      }
//    })
    val rdd2 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==3)
//        println("##############################!=4#############"+rdd2.count())
//
//    val rdd3 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==3)
//    println("##############################==3#############"+rdd3.count())
//    val rdd4 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==2)
//    println("##############################==2#############"+rdd4.count())
//    val rdd5 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==1)
//    println("##############################==1#############"+rdd5.count())
//    val rdd6 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==0)
//    println("##############################==0#############"+rdd6.count())

    var a = rdd2.toArray()
    for(i <- 0 to a.length-1){
      println("#######=====3333##############     "+i+"     "+a(i))
    }

    val rdd3 = outclinical_diago_rdd.map(_.split("\001")).filter(_.length==1)
    var b = rdd3.toArray()
    for(i <- 0 to b.length-1){
      println("#######=====11111##############     "+i+"     "+b(i))
    }




//    val outclinical = outclinical_diago_rdd.map(line =>{
//      var strs = line.split("\001")
//
//      if(strs.length==1){
//      println("######################==1"+line)
//      }
//  if(strs.length==3){
//    println("######################==3"+line)
//  }

    //outclinical.repartition(1)saveAsTextFile("hdfs://10.2.8.11:8020/user/hive/warehouse/fasle")

    /*//搜索结果排名第1，但是点击次序排在第2的数据有多少?
    val rdd1 = sc.textFile("hdfs://hadoop1:8000/dataguru/data/SogouQ1.txt")
    val rdd2=rdd1.map(_.split("\t")).filter(_.length!=4)
    rdd2.count()
    val rdd3=rdd2.filter(_(3).toInt==1).filter(_(4).toInt==2)
    rdd3.count()
    rdd3.toDebugString*/
  }
}
