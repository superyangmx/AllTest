package scalaimpl

import java.text.SimpleDateFormat
import java.util.Date

import Mysql.ReadProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Success
import scalaTextfile.Textfile

trait AbnormalDataDelet {


/*
根据某列，去除不合理的数据
 */

   def abnormaldatadelet(sc:SparkContext,rules:List[String],files:String,outputfile:String,columnnumber: Int,num1:Int,num2:Int,myflat:Int,mark:String):String={
     println("进入到合理范围过滤方法中")
     println("参数：files："+files+" outputfile :"+outputfile+" columnnumber :"+columnnumber+" num1 :"+num1+ " num2 :"+num2+" mark :"+mark)
     val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     //设置日期格式
     val time2 = df2.format(new Date())
     val readProperties = new ReadProperties()
     val hdfs3 = readProperties.get("hdfs")
//     val conf = new SparkConf()
//       .setAppName("CleanFile")
//         //.setMaster("spark://data1.cshdp.com:7077")
//       //.setMaster("local")
//       .set("spark.driver.allowMultipleContexts","true")
//     val sc = new SparkContext(conf)
     val Tf= new Textfile
     var flat =0
     val rule = "42"
     val ss = rules.indexOf(rule) +myflat-1
     var file = sc.textFile("")
     //判断接收文件还是接受前一个方法的结果
     println("ss :"+ss)
    try{
     if (ss == 0) {
       file= sc.textFile(files)
       println("合理范围过滤方法 inputfile is :"+files)
     }
     if (ss >0){
        file=sc.textFile(Tf.select(ss,rule,rules,myflat)).cache()
       println("合理范围过滤方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
     }

      val numMax = file.map(line => {
          var aa = line.split(mark, -1).length
          aa
        }).max()
     println("numMax is :"+numMax)
     if(columnnumber+1 >numMax){
       print("HLFWGL:超出范围，重新选择")
       sc.stop()
       "fail"
     }
     else {
//按范围过滤
       println("columnnumber is :"+columnnumber)
       val Number = columnnumber

       val resultabn = file.map(line => {
         val aa = line.split(mark, -1)

         val r1 = scala.util.Try(aa(Number).toInt)
         val result = r1 match {
           case Success(_) => 10086;
           case _ => 10010
         }
         if (result == 10086) {
           val bb = aa(Number).toInt
               if (num1 <= bb && bb <= num2) {
                 line}
               else{
                 "null"}
         } else if (result == 10010) {
           "HLFWGL:出错，字段不是数字型"

         } else "出错"
       }).filter(x => x != "null").map(lines => (lines.trim, " ")).groupByKey().sortByKey().keys
       println("合理范围过滤操作完成")
 //合理范围过滤数据条数计数
       val resnum = file.map(line =>{
         val aa = line.split(mark, -1)
         val r1 = scala.util.Try(aa(Number).toInt)
         val result = r1 match {
           case Success(_) => 10086;
           case _ => 10010
         }
         if (result == 10086) {
           val bb = aa(Number).toInt
           if (num1 <= bb && bb <= num2) {
             line}
           else{
             "null"}
         }else if (result == 10010) {
           "HLFWGL:出错，字段不是数字型"

         } else "出错"
       }).filter(x=>x!="null").count()
       println("合理范围过滤计数完成")
  /*
不规范数据
       */
       val resultfalse = file.map(line => {
         val aa = line.split(mark,-1)

         val r1 = scala.util.Try(aa(Number).toInt)
         val result = r1 match {
           case Success(_) => 10086;
           case _ => 10010
         }
         if (result == 10086) {
           val bb = aa(Number).toInt
               if (bb <=num1 || bb >= num2) {
                 line}
               else{
                 "null"}
         } else if (result == 10010) {
           "HLFWGL:出错，请正确选择列"

         } else "出错"
       }).filter(x => x != "null").map(lines => (lines.trim, " ")).groupByKey().sortByKey().keys
       println("合理范围过滤不规范数据过滤完成")

//存储路径判断
       if (ss < rules.length - 1) {
         val conf11 = new Configuration()
         val fs = FileSystem.get(conf11)
         if (fs.exists(new Path(hdfs3+"/test/HLFWGL"+myflat))) {
           fs.delete(new Path(hdfs3+"/test/HLFWGL"+myflat), true)
         }
         resultabn.saveAsTextFile(hdfs3+"/test/HLFWGL"+myflat)
         println("合理范围过滤保存路径 :"+hdfs3 +"/test/HLFWGL"+myflat)

       } else {
         val conf11 = new Configuration()
         val fs = FileSystem.get(conf11)
         if (fs.exists(new Path(outputfile))) {
           fs.delete(new Path(outputfile), true)
         }

         resultabn.coalesce(1, true).saveAsTextFile(outputfile)
         println("合理范围过滤保存路径 :"+outputfile)
       }

       val conf12 = new Configuration()
       val fs = FileSystem.get(conf12)
       if (fs.exists(new Path(hdfs3+"/test/HLFWGLfalse"))) {
         fs.delete(new Path(hdfs3+"/test/HLFWGLfalse"), true)
       }
       resultfalse.coalesce(1, true).saveAsTextFile(hdfs3+"/test/HLFWGLfalse")
       println("不规范数据保存路径 :"+hdfs3+"/test/HLFWGLfalse")
       //sc.stop()

       resnum.toString
     }
    }catch{
      case ex:Exception=>{
       println("HLFWGL is fail the message is:"+time2+" "+ex)
        sc.stop()
        "fail"
      }
    }
    }
}
