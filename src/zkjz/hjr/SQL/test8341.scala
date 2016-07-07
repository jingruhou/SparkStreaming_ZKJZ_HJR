package zkjz.hjr.SQL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/6/15.
  */
object test8341 {
  def main(args:Array[String]): Unit ={
    //初始化配置
    val conf = new SparkConf().setAppName("zkjz_hjr_check").setMaster("local")
    val sc = new SparkContext(conf)

    //加载文件
    val outclinical_diago_rdd = sc.textFile("C:/Users/Administrator.PC-201512221019/Desktop/门诊表相关数据/outclinical_diag2.txt")
    val outclinical_words_rdd = sc.textFile("C:/Users/Administrator.PC-201512221019/Desktop/词库表/001.csv")

    //将词库表转化为数组
    val counts_word = outclinical_words_rdd.toArray()

    //将转化为数组的词库表放入集合中
    var diag = ""
    var words =""
    var map = Map(diag -> words)//(0)将词库表转化为一个Map（门诊诊断->ICD_CODE）
    for(i <- 0 to counts_word.length - 1){
      var line = counts_word(i)

      if(line.split("\\$").length == 2){

        diag = line.split("\\$")(0)
        words = line.split("\\$")(1)
        map += (diag -> words)
      }
      else
        println("异常数据： "+line)

    }
   /* counts_word.map(line =>{
      //判断行的数据的 列数
      diag = line.split("\t")(0)
      words = line.split("\t")(1)
      map += (diag -> words)
    })*/


    //处理门诊表的业务逻辑
    val outclinical = outclinical_diago_rdd.filter(_.split("\001").length == 4).map(line =>{
      var strs = line.split("\001")
      var l = strs(3)
      var firstline = strs(0)+"\001"+strs(1)+"\001"+strs(2)+"\001"//（门诊表业务逻辑---2）拿出前三个字段
      var lastline = ""
      var s = line
      var m = l.length
      while (m >= 1) {
        var j = 0
        while (j < l.length - m + 1) {
          var s3 = l.substring(j, j + m)
          if (map.contains(s3)) {
            //s=s.replace(s3,map(s3)+" ")
            lastline += map(s3)+","
            l = l.replace(s3, "")
          }
          j = j + 1
        }
        m = m - 1
      }
      //firstline+lastline
      //数据校验
      line+"\001"+lastline
    })
    outclinical.repartition(1).saveAsTextFile("C:/Users/Administrator.PC-201512221019/Desktop/词库表/001Result")
  }
}
