package zkjz.hjr.SQL

import breeze.linalg.diag
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
/**
  * Created by Administrator on 2016/5/30.
  */
object SQLzkjz {

  def main(args:Array[String]): Unit ={
    // 0 初始化配置
    val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("ZKJZ_SQL").setMaster("spark://10.2.1.111:7077")
    val sc = new SparkContext(conf)

    // 1 创建sqlContext
    var sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 2 创建RDD
    val outclinical_diago_rdd = sc.textFile("D://streamingData//sql//outclinical_diago530.txt")
    val outclinical_words_rdd = sc.textFile("D://streamingData//sql//outclinical_words.txt")

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
    //map.foreach(print)


    //将outclinical_diago_rdd数据集转化为数组
    var counts = outclinical_diago_rdd.flatMap(line => line.split("\n")).toArray()

    for(i <- 0 to counts.length-1){
      var line = counts(i)
      //println(line)
      var l = line.split("\t")(3)
      var s = line.split("\t")(0)+"\t"+line.split("\t")(1)+"\t"+line.split("\t")(2)+"\t"
     //println(i)
      //println(s)
      //println(l.length)
//      for(m <- l.length() to 1){
//          println(m)
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
      println(s)
    }
//    line = stra;
//    String l=line.split(d)[3];
//
//    String s = line.split(d)[0]+"," + line.split(d)[1]+","+ line.split(d)[2]+"--";
//
//    for(int i =l.length();i >= 1;i--) {
//
//      for (int j = 0; j < l.length() - i + 1; j++) {
       //var j=0
        //while(j<l.length()-m+1){
    //
    //
    //j=j+1
    // }


//        String s3 = l.substring(j, j + i);
//        if (map.containsKey(s3)) {
//          s += map.get(s3) + ".";
//          l = l.replace(s3, "");
//        }
//      }
//    }
//    System.out.println(s);





    //outclinical_diago_rdd.map(lambda x: x).collect()
    //val s = outclinical_diago_rdd.map(line => line.split('\t').toList)

    //println(outclinical_diago_rdd.first())

   // println(s.count())

   // s.foreach(println)

   // s.collect()
    //val test = outclinical_diago_rdd.toString().split("\n")

   // test.foreach(println)

    //lines = sc.textFile(your file path)

   /* //foreach循环打印数据
    outclinical_diago_rdd.foreach(println)
    outclinical_words_rdd.foreach(println)

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

    // 9 sql测试
    val outclinical_diago1 = sqlContext.sql("select rid, mpi_persion_id, visitor_date, out_diag from outclinical_diago")
    outclinical_diago1.printSchema()
    outclinical_diago1.map(t => "列rid: " + t(0)).collect().foreach(println)
    outclinical_diago1.map(t => "列mpi_persion_id: " + t(1)).collect().foreach(println)
    outclinical_diago1.map(t => "列visitor_date: " + t(2)).collect().foreach(println)
    outclinical_diago1.map(t => "列out_diag: " + t(3)).collect().foreach(println)

    val outclinical_words1 = sqlContext.sql("select diag_name, icd_code, lengthw from outclinical_words")
    outclinical_words1.printSchema()
    outclinical_words1.map(t => "列diag_name: " + t(0)).collect().foreach(println)
    outclinical_words1.map(t => "列icd_code: " + t(1)).collect().foreach(println)
    outclinical_words1.map(t => "列lengthw: " + t(2)).collect().foreach(println)

    // 10 sql 逻辑
    val result = sqlContext.sql("select mpi_persion_id,visitor_date,icd_code " +
      "from outclinical_diago,outclinical_words " +
      "where out_diag=diag_name order by visitor_date desc")
    result.show()*/


    /*//两张表连接
    val joinResult = outclinical_diago1 join(outclinical_words1)
    //打印前100条数据
    joinResult.take(100)*/


 /*
    //分析数据
    static String data = "1,9829135,2010-4-20,浮肿查因/高血压";
    static HashMap<String, String>  map= new HashMap<String, String>();

    public static String runData(String line,HashMap<String, String> map){
      String s = line.split(",")[0]+"," + line.split(",")[1]+","+ line.split(",")[2]+",";
      for(int i =line.split(",")[3].length();i >= 1;i--){//每次加一个字，从一个字开始
        for(int j=0;j<line.split(",")[3].length()-i+1;j++){
          String s3 = line.split(",")[3].substring(j, j+i);
          if(map.containsKey(s3)){
            s += map.get(s3)+".";
            line = line.replace(s3, "");
            break;
          }
        }
      }
      System.out.println(s);
      return s;
    }

    public static void main(String []args){
      map.put("浮肿查因","code1");
      map.put("高血压","code2");
      map.put("高血","code3");
      runData(data,map);
    }*/

  }
}
