package zkjz.hjr.SQL

import breeze.linalg.split
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/6.
  */
object test1 {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    //（1）过滤掉第一列
    val pairsRdd = outclinical_diago_rdd.map(line =>{
      val col1 = line.split("\t")
      col1(1) +"\t"+ col1(2) +"\t"+ col1(3)
    })
    pairsRdd.foreach(println)

    //（2）使用第一MPI_PERSION_ID和第二个VISITOR_DATE字段作为key
    val pairs = outclinical_diago_rdd.map(line => {
      val col2 = line.split("\t")
      (col2(1)+col2(2),col2(3))
    })
    pairs.foreach(println)

    //（3）reduceByKey过程 val counts = pairs.reduceByKey((a, b) => a + b)
    val result = pairs.reduceByKey((a,b) => a+b)
    result.foreach(println)


    //关闭sc
    sc.stop()
  }
}
