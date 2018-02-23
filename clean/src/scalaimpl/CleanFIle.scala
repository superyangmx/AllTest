package scalaimpl

import java.text.SimpleDateFormat
import java.util.Date

import Mysql.ReadProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scalaTextfile.Textfile

trait CleanFIle {

    def cleanfile(sc:SparkContext,rules:List[String],files:String,outputfile:String) :String= {
      val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val time1 = df1.format(new Date())
      val readProperties = new ReadProperties()
      val hdfs4 = readProperties.get("hdfs")
      println("进入去重方法==============")
//     val conf =new SparkConf()
//       // .setMaster("spark://data1.cshdp.com:7077")
//       //.setMaster("local")
//        .setAppName("cleanfile")
//       .set("spark.driver.allowMultipleContexts", "true")
//      val sc = new SparkContext(conf)
      val rule = "41"
      var file = sc.textFile("")
      val Tf= new Textfile
     //判断接收文件还是接受前一个方法的结果
     val ss = rules.indexOf(rule)
 try {
   if (ss == 0) {
     file = sc.textFile(files).cache()
     println("去重方法 inputfile is :"+files)
   }
   if (ss > 0) {
     file = sc.textFile(Tf.selectALL(ss, rule, rules)).cache()
     println("去重方法 inputfile is :"+Tf.selectALL(ss, rule, rules))
   }

   val resultRddin = file
     .map(line => (line.trim, ""))
     .groupByKey().cache()

   //  去重
   val resultRdd = resultRddin.sortByKey().keys
  println("去重操作完成")
//去重的数据条数计数
   val resnum1=resultRddin.count()
   val resnum2 = file.count()
   val resnum = resnum2 - resnum1
   println("去重计数完成")

   if (ss < rules.length - 1) {
     val conf11 = new Configuration()
     val fs = FileSystem.get(conf11)
     if (fs.exists(new Path( hdfs4 +"/test/QC"))) {
       fs.delete(new Path(hdfs4 +"/test/QC"), true)
     }
     resultRdd.saveAsTextFile(hdfs4 +"/test/QC")
      println("去重保存路径 :"+hdfs4 +"/test/QC")
   } else {
     val conf11 = new Configuration()
     val fs = FileSystem.get(conf11)
     if (fs.exists(new Path(outputfile))) {
       fs.delete(new Path(outputfile), true)
     }

     resultRdd.coalesce(1, true).saveAsTextFile(outputfile)
     println("去重保存路径 : "+outputfile)
   }

 // sc.stop()
   resnum.toString
 }catch {
   case ex:Exception=> {
     print(time1+" QC is fail the message is "+ex)
     sc.stop()
     "fail"
   }
 }

    }
///////////////////////////////////////////////////////////////////////////////////////////
  //按字段删除空值

  def cleannull(sc:SparkContext,rules:List[String],files:String,outputfile:String,field_name:String,myflat:Int,mark:String) :String={
    println("进入到空值删除方法中")
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val time1 = df1.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs4 = readProperties.get("hdfs")
      //去空
//       val conf = new SparkConf()
//         //.setMaster("spark://data1.cshdp.com:7077")
//         //.setMaster("local")
//         .setAppName("CleanNull")
//         .set("spark.driver.allowMultipleContexts", "true")
//       val sc = new SparkContext(conf)
    val Tf= new Textfile
    var flat=0
    val rule = "11"
    val ss = rules.indexOf(rule) +myflat-1
    var file = sc.textFile("")
//判断接收文件还是接受前一个方法的结果
try{
    if (ss == 0) {
      file= sc.textFile(files).cache()
      println("空值删除方法 inputfile is :"+files)
    }

    if (ss >0){
    file=sc.textFile(Tf.select(ss,rule,rules,myflat)).cache()
      println("空值删除方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
    }

//方法
    val num =field_name.toInt
      val inputRdd1=file
        .map(line =>{
          val aa = line.split(mark,-1)
          if (aa(num)!=""&& aa(num)!="NULL")
            line
          else
            "null"
        })
        .filter(x =>x!="null").map(lines =>(lines.trim," ")).sortByKey().keys
    println("空值删除操作完成")
//空值删除的数据数量
val resnum=file.map(line =>{
    val aa = line.split(mark,-1)
    if (aa(num)!="NULL" && aa(num)!="")
      "null"
    else
      "aa"
  }).filter(x =>x!="null").count()
    println("空值删除计数完成")
//判断结果输出还是存入临时文件

    if (ss < rules.length - 1) {
      val conf11 = new Configuration()
      val fs = FileSystem.get(conf11)
      if (fs.exists(new Path(hdfs4 +"/test/QK"+myflat))) {
        fs.delete(new Path(hdfs4 +"/test/QK"+myflat), true)
      }
      inputRdd1.saveAsTextFile(hdfs4 +"/test/QK"+myflat)
      println("空值删除保存路径 :"+hdfs4 +"/test/QK"+myflat)

    } else {
      val conf11 = new Configuration()
      val fs = FileSystem.get(conf11)
      if (fs.exists(new Path(outputfile))) {
        fs.delete(new Path(outputfile), true)
      }

      inputRdd1.coalesce(1, true).saveAsTextFile(outputfile)
      println("空值删除保存路径 :"+outputfile)
    }
           // sc.stop()
          resnum.toString
}catch {
      case ex:Exception=>{
      print(time1+" QK is fail ,the message is: " +ex)
        sc.stop()
        "fail"
      }
    }

    }
///////////////////////////////////////////////////////////////////////////////////////////////
/*
空值填充
 */

  def filldata(sc:SparkContext,rules:List[String],files:String,outputfile:String,field_name:String,fill_content:String,myflat:Int,mark:String):String={
    println("进入到空值填充方法中")
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val time1 = df1.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs4 = readProperties.get("hdfs")
//    val conf = new SparkConf()
//     .setAppName("FillingData")
//     // .setMaster("local")
//      //.setMaster("spark://data1.cshdp.com:7077")
//      .set("spark.driver.allowMultipleContexts","true")
//    val sc = new SparkContext(conf)
    val Tf= new Textfile
    var flat=0
    val rule = "12"
    val ss = rules.indexOf(rule) +myflat-1
    var file = sc.textFile("")
    //判断接收文件还是接受前一个方法的结果
   try {
     if (ss == 0) {
       file = sc.textFile(files).cache()
       println("空值填充方法 inputfile is :"+files)
     }
     if (ss > 0) {
       file = sc.textFile(Tf.select(ss, rule, rules, myflat)).cache()
       println("空值填充方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
     }
println("filepath is :"+file)
     /*
    填充空值
     */
     val num = field_name.toInt
     val input = file.map(line => {
       val aa = line.split(mark, -1)
       if (aa(num) == "" || aa(num) == "null"|| aa(num)=="NULL") {
         aa.updated(num, fill_content) mkString (mark)
       } else {
         line
       }
     })
     println("空值填充操作完成")
//空值填充的数据条数
val resnum = file.map(line => {
  val aa = line.split(mark, -1)
  if (aa(num) == "" || aa(num) == "null"|| aa(num)=="NULL" ) {
      "aaa"
  } else {
   "null"
  }
}).filter(x=>x!="null").count()
     println("空值填充计数完成")
     //输出判断
     if (ss < rules.length - 1) {
       val conf11 = new Configuration()
       val fs = FileSystem.get(conf11)
       if (fs.exists(new Path(hdfs4 +"/test/KZTC" + myflat))) {
         fs.delete(new Path(hdfs4 +"/test/KZTC" + myflat), true)
       }
       input.saveAsTextFile(hdfs4 +"/test/KZTC" + myflat)
       println("空值填充保存路径 :"+hdfs4 +"/test/KZTC"+myflat)

     } else {
       val conf11 = new Configuration()
       val fs = FileSystem.get(conf11)
       if (fs.exists(new Path(outputfile))) {
         fs.delete(new Path(outputfile), true)
       }

       input.coalesce(1, true).saveAsTextFile(outputfile)
       println("空值填充保存路径 :"+outputfile)
     }
          //sc.stop()
        resnum.toString
   }catch{
     case ex:Exception=>{

       print(time1+" KZTC is fail ,the meaasge is : "+ex)
       sc.stop()
       "fail"
     }
   }


  }
}
