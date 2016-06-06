package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/5/30.
  */
object WordCount {
  def main(args: Array[String]) {
         if (args.length < 1) {
             System.err.println("Usage: <file>")
             System.exit(1)
          }
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val line = sc.textFile(args(0))

        line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

        sc.stop()
      }
}
