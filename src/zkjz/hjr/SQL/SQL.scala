package zkjz.hjr.SQL

import org.apache.poi.ss.formula.functions.T
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/5/30.
  */
object SQL {
  def main(args:Array[String]): Unit = {
    // 0 初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("spark://10.2.1.111:7077")
    val sc = new SparkContext(conf)

    // 1 创建sqlContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // 2 创建RDD
    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    val outclinical_words_rdd = sc.textFile("D://streamingData//sql//outclinical_words.txt")
    println("outclinical_diago_rdd: " + outclinical_diago_rdd.count() + " 条数据")
    println("outclinical_words_rdd: " + outclinical_words_rdd.count() + " 条数据")
    // 3 创建Schema String
    val outclinical_diago_schemaString = "rid mpi_persion_id visitor_date out_diag"
    val outclinical_words_schemaString = "diag_name icd_code lengthw"
    // 4 通过SchemaString生成Schema
    val outclinical_diago =
      StructType(
        outclinical_diago_schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val outclinical_words =
      StructType(
        outclinical_words_schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // 5 将RDD的记录强制转换为行记录
    val rowRDD1 = outclinical_diago_rdd.map(_.split("\t")).map(r1 => Row(r1(0),r1(1),r1(2),r1(3)))
    val rowRDD2 = outclinical_words_rdd.map(_.split("\t")).map(r2 => Row(r2(0),r2(1),r2(2)))
    // 6 将schema应用到RDD
    val outclinical_diago_DataFrame = sqlContext.createDataFrame(rowRDD1, outclinical_diago)
    val outclinical_words_DataFrame = sqlContext.createDataFrame(rowRDD2, outclinical_words)
    // 7 将DataFrame注册成为表
    outclinical_diago_DataFrame.registerTempTable("outclinical_diago")
    outclinical_words_DataFrame.registerTempTable("outclinical_words")

    // 8 测试
    outclinical_diago_DataFrame.show()
    outclinical_words_DataFrame.show()

    // 9-1 拿出需要匹配的两个DataFrame的列
    val col_out_diag = outclinical_diago_DataFrame.select("out_diag")
    val col_diag_name = outclinical_words_DataFrame.select("diag_name")

    // 9-2 两个列的行匹配
    /*col_out_diag match {
      //case  =>
    }*/
    //自定义打印行数据
    def colFun1(): Unit ={
      col_diag_name.foreach(println)
    }
  }
}
