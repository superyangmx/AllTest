package scalaimpl

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import Mysql.ReadProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scalaTextfile.Textfile

trait IDandPhone {

  def idsecret(sc:SparkContext,rules:List[String],files:String,outputfile:String,field_name:String,myflat:Int,mark:String):String={
    println("进入到身份证加密方法中")
    val df4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time4 = df4.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs5 = readProperties.get("hdfs")
//    val conf = new SparkConf()
//      //.setMaster("spark://data1.cshdp.com:7077")
//      //  .setMaster("local")
//      .setAppName("idRDD")
//      .set("spark.driver.allowMultipleContexts", "true")
//    val sc = new SparkContext(conf)
    val rule = "33"
    var flat=0
    var file = sc.textFile("")
    val Tf= new Textfile
    val ss = rules.indexOf(rule) +myflat-1
    try {
      if (ss == 0) {
        file = sc.textFile(files).cache()
        println("身份证加密方法 inputfile is :"+files)
      }
      if (ss > 0) {
        file = sc.textFile(Tf.select(ss, rule, rules, myflat)).cache()
        println("身份证加密方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
      }
      //身份证号码加密
      val num = field_name.toInt
      val p = Pattern.compile("^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$")
      val idres = file.map(line => {
        val aa = line.split(mark, -1)
        val m = p.matcher(aa(num))
        if (m.matches()) {
          line.replace(aa(num),"*")
        }
        //aa(0).replaceAll("^(\\d{6})(\\d{4})(\\d{2})(\\d{2})(\\d{3})([0-9]|X)$","*")
        else {
          line
        }
      })
      println("身份证加密操作完成")
      //计算加密了多少条数据
      val idresnum = file.map(line => {
        val aa = line.split(mark,-1)
        val m = p.matcher(aa(num))
        if (m.matches()) {
          "aa"
        }
        else {
          "null"
        }
      }).filter(x => x != "null").count()
      println("身份证加密计数完成")
      //输出路径判断
      if (ss < rules.length - 1) {
        val conf11 = new Configuration()
        val fs = FileSystem.get(conf11)
        if (fs.exists(new Path(hdfs5 +"/test/IDres" + myflat))) {
          fs.delete(new Path(hdfs5 +"/test/IDres" + myflat), true)
        }
        idres.saveAsTextFile(hdfs5 +"/test/IDres" + myflat)
        println("身份证加密保存路径 :"+hdfs5 +"/test/ID"+myflat)

      } else {
        val conf11 = new Configuration()
        val fs = FileSystem.get(conf11)
        if (fs.exists(new Path(outputfile))) {
          fs.delete(new Path(outputfile), true)
        }

        idres.coalesce(1, true).saveAsTextFile(outputfile)
        println("身份证加密保存路径 :"+outputfile)
      }
     // sc.stop()
      idresnum.toString
    }catch {
      case ex: Exception => {
        print(time4+" IDres is fail ,the message is : "+ex)
        sc.stop()
        "fail"
      }
    }
    }

//////////////////////////////////////////手机号加密=================================
  def phonesecret(sc:SparkContext,rules:List[String],files:String,outputfile:String,field_name:String,myflat:Int,mark:String):String={
    println("进入到手机号码加密方法中")
    val df4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time4 = df4.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs5 = readProperties.get("hdfs")
//    val conf = new SparkConf()
//    // .setMaster("spark://data1.cshdp.com:7077")
//      //.setMaster("local")
//      .setAppName("phoneRDD")
//      .set("spark.driver.allowMultipleContexts", "true")
//    val sc = new SparkContext(conf)
    val rule = "34"
    var flat=0
    var file = sc.textFile("")
    val Tf= new Textfile
    val ss = rules.indexOf(rule) +myflat-1
 try {
   if (ss == 0) {
     file = sc.textFile(files).cache()
     println("手机号码加密方法 inputfile is :"+files)
   }
   if (ss > 0) {
     file = sc.textFile(Tf.select(ss, rule, rules, myflat)).cache()
     println("手机号码加密方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
   }
   //电话号码加密
   val num = field_name.toInt
   val p = Pattern.compile("^1(3|4|5|7|8)\\d{9}$")
   val phoneres = file.map(line => {
     val aa = line.split(mark,-1)
     val m = p.matcher(aa(num))
     if (m.matches()) {
       line.replace(aa(num),"*")
     } else {
       line
     }
   })
   println("手机号码加密操作完成")
   //计算加密了多少条数据
   val phoneresnum = file.map(line => {
     val aa = line.split(mark,-1)
     val m = p.matcher(aa(num))
     if (m.matches()) {
       "aa"
     } else {
       "null"
     }
   }).filter(x => x != "null").count()
   println("手机号码加密计数完成")
   //判断输入路径
   if (ss < rules.length - 1) {
     val conf11 = new Configuration()
     val fs = FileSystem.get(conf11)
     if (fs.exists(new Path(hdfs5 +"/test/Phoneres" + myflat))) {
       fs.delete(new Path(hdfs5 +"/test/Phoneres" + myflat), true)
     }
     phoneres.saveAsTextFile(hdfs5 +"/test/Phoneres" + myflat)
     println("手机号码加密保存路径 :"+hdfs5+"/test/Phoneres" + myflat)

   } else {
     val conf11 = new Configuration()
     val fs = FileSystem.get(conf11)
     if (fs.exists(new Path(outputfile))) {
       fs.delete(new Path(outputfile), true)
     }

     phoneres.coalesce(1, true).saveAsTextFile(outputfile)
     println("手机号码加密保存路径 :"+outputfile)
   }
      //sc.stop()
   phoneresnum.toString
 }catch {
   case ex: Exception => {
     print(time4+" Phoneres is fail ,the message is :"+ex)
     sc.stop()
     "fail"
   }
 }

  }


}
