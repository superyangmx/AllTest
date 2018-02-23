package scalaimpl

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import Mysql.ReadProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scalaTextfile.Textfile

trait SensitiveWordDelet {

//敏感词删除
  def sensitivewordelet(sc:SparkContext,rules:List[String],files:String,outputfile:String,sensitiveword:String,mark:String):String={
    println("进入敏感词删除方法")
    val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val time3 = df3.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs6 = readProperties.get("hdfs")
//      val conf = new SparkConf()
//           // .setMaster("spark://data1.cshdp.com:7077")
//         // .setMaster("local")
//            .setAppName("SensitiveWordDelet ")
//            .set("spark.driver.allowMultipleContexts","true")
//          val sc = new SparkContext(conf)

          val Tf= new Textfile
          var flat =0
          val rule = "31"
          val ss = rules.indexOf(rule)
          var file = sc.textFile("")
          //判断接收文件还是接受前一个方法的结果
    try {
      if (ss == 0) {
        file = sc.textFile(files).cache()
        println("敏感词删除方法 inputfile is :"+files)
      }
      if (ss > 0) {
        file = sc.textFile(Tf.selectALL(ss, rule, rules)).cache()
        println("敏感词删除方法 inputfile is :"+Tf.selectALL(ss, rule, rules))
      }
 //方法
      val res1 = file.map(line => {
        val aa = line.split(mark, -1).contains(sensitiveword)
        if (aa == true) {
          "null"
        } else {
          line
        }
      }).filter(x => x != "null")
      println("敏感词删除操作完成")
//敏感词删除数据条数
val resnum = file.map(line => {
  val aa = line.split(mark, -1).contains(sensitiveword)
  if (aa == true) {
    "num"
  } else {
    "null"
  }
}).filter(x => x != "null").count()
      println("敏感词删除计数完成")
//保存路径判断
      if (ss < rules.length - 1) {
        val conf11 = new Configuration()
        val fs = FileSystem.get(conf11)
        if (fs.exists(new Path(hdfs6+"/test/MGCSC"))) {
          fs.delete(new Path(hdfs6+"/test/MGCSC"), true)
        }
        res1.saveAsTextFile(hdfs6+"/test/MGCSC")
        println("敏感词删除保存路径 :"+hdfs6 +"/test/MGCSC")

      } else {
        val conf11 = new Configuration()
        val fs = FileSystem.get(conf11)
        if (fs.exists(new Path(outputfile))) {
          fs.delete(new Path(outputfile), true)
        }
        res1.coalesce(1, true).saveAsTextFile(outputfile)
        println("敏感词删除保存路径 :"+outputfile)
      }
      //sc.stop()
      resnum.toString
    }catch {
      case ex:Exception=>{
       println(time3+"MGCSC is fail , the message is : "+ex)
        sc.stop()
        "fail"
      }
    }

  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//敏感词替换
  def sensitivewordreplace(sc:SparkContext,rules:List[String],files:String,outputfile:String,sensitiveword:String,replaceword:String,mark:String):String={
  println("进入到敏感词替换方法中")
  val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //设置日期格式
  val time3 = df3.format(new Date())
  val readProperties = new ReadProperties()
  val hdfs6 = readProperties.get("hdfs")
//          val conf = new SparkConf()
//            //.setMaster("spark://data1.cshdp.com:7077")
//            .setAppName("SensitiveWordReplace")
//            .set("spark.driver.allowMultipleContexts","true")
//          val sc = new SparkContext(conf)

        val Tf= new Textfile
        var flat =0
        val rule = "32"
        val ss = rules.indexOf(rule)
        var file = sc.textFile("")
//判断接收文件还是接受前一个方法的结果
  try {
    if (ss == 0) {
      file = sc.textFile(files).cache()
      println("敏感词替换方法 inputfile is :"+files)
    }
    if (ss > 0) {
      file = sc.textFile(Tf.selectALL(ss, rule, rules)).cache()
      println("敏感词替换方法 inputfile is :"+Tf.selectALL(ss, rule, rules))
    }

///方法/////////////
    val res2 = file.map(line => {
      val AA = line.split(mark, -1)
      val aa = line.split(mark, -1).indexOf(sensitiveword)
      if (aa < 0) {
        line
      } else {
        line.replace(AA(aa), replaceword)
      }
    })
    println("敏感词替换操作完成")
//敏感词替换的数据条数
val resnum = file.map(line => {
  val AA = line.split(mark, -1)
  val aa = line.split(mark, -1).indexOf(sensitiveword)
  if (aa < 0) {
    "null"
  } else {
    line.replace(AA(aa), replaceword)
  }
}).filter(x=>x!="null").count()
    println("敏感词替换计数完成")
//判断输出路径
    if (ss < rules.length - 1) {
      val conf11 = new Configuration()
      val fs = FileSystem.get(conf11)
      if (fs.exists(new Path(hdfs6+"/test/MGCTH"))) {
        fs.delete(new Path(hdfs6+"/test/MGCTH"), true)
      }
      res2.saveAsTextFile(hdfs6+"/test/MGCTH")
      println("敏感词替换保存路径 :"+hdfs6 +"/test/MGCTH")

    } else {
      val conf11 = new Configuration()
      val fs = FileSystem.get(conf11)
      if (fs.exists(new Path(outputfile))) {
        fs.delete(new Path(outputfile), true)
      }
      res2.coalesce(1, true).saveAsTextFile(outputfile)
      println("敏感词替换保存路径 : "+outputfile)
    }
   // sc.stop()
    resnum.toString
  }catch {
    case ex: Exception => {
      print(time3+" MGCTH is fail ,the message is :"+ex)
      sc.stop()
      "fail"
    }
  }

  }

  //时间格式化
  def dateformat(sc:SparkContext,rules:List[String],files:String,outputfile:String,fields_name:String,old_format:String,new_format:String,myflat:Int,mark:String):String={
    println("进入到格式替换方法中")
    val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val time3 = df3.format(new Date())
    val readProperties = new ReadProperties()
    val hdfs6 = readProperties.get("hdfs")
//          val conf = new SparkConf()
//            .setAppName("dateformat")
//          //.setMaster("spark://data1.cshdp.com:7077")
//          val sc = new SparkContext(conf)
          val Tf = new Textfile

          var flat = 0
          val rule = "2"
          val ss = rules.indexOf(rule) + myflat - 1
          var file = sc.textFile("")
          //判断接收文件还是接受前一个方法的结果
    try {
          if (ss == 0) {
            file = sc.textFile(files).cache()
            println("格式替换方法 inputfile is :"+files)
          }
          if (ss > 0) {
            file = sc.textFile(Tf.select(ss, rule, rules, myflat)).cache()
            println("格式替换方法 inputfile is :"+Tf.select(ss,rule,rules,myflat))
          }

          ////////////////////////////////////////////////////////////////////////////////方法
          var input :RDD[String] = null
          val old_format4= old_format.substring(0,4)
          val p = Pattern.compile("\\d{4}")
          if (old_format4 == "yyyy") {
            print("aaaaaaaaaaaaaaaa")
            input=file.map(line => {
              val aa = line.split(mark, -1)
              val m = p.matcher(aa(fields_name.toInt).substring(0, 4))
              if (m.matches()) {
                try {
                  val date = new SimpleDateFormat(old_format).parse(aa(fields_name.toInt))
                  val res = new SimpleDateFormat(new_format).format((date))
                  aa.updated(fields_name.toInt, res).mkString(mark)
                } catch {
                  case ex: Exception => {
                    line
                  }
                }
              } else {line}
            })
          } else {
            input= file.map(line => {
              val aa = line.split(mark, -1)
              val m = p.matcher(aa(fields_name.toInt).substring(0, 4))
              println(aa(fields_name.toInt).substring(0,4))
              if (m.matches()) {
                line
              } else {
                try {
                  val date = new SimpleDateFormat(old_format).parse(aa(fields_name.toInt))
                  val res = new SimpleDateFormat(new_format).format((date))
                  aa.updated(fields_name.toInt, res).mkString(mark)
                } catch {
                  case ex: Exception => {
                    line
                  }
                }
              }
            })
          }

          println("格式替换操作完成")
          ///////////////////////////计算格式化多少条数据///////////////////////
          var resnum :Long=0
          if (old_format4 == "yyyy") {
            print("aaaaaaaaaaaaaaaa")
            resnum = file.map(line => {
              val aa = line.split(mark, -1)
              val m = p.matcher(aa(fields_name.toInt).substring(0, 4))
              if (m.matches()) {
                try {
                  val date = new SimpleDateFormat(old_format).parse(aa(fields_name.toInt))
                  val res = new SimpleDateFormat(new_format).format((date))
                  //aa.updated(fields_name.toInt, res).mkString(mark)
                  "success"
                } catch {
                  case ex: Exception => {
                    "null"
                  }
                }
              } else {"null"}
            }).filter(x=>x!="null").count()
          } else {
            resnum = file.map(line => {
              val aa = line.split(mark, -1)
              val m = p.matcher(aa(fields_name.toInt).substring(0, 4))
              println(aa(fields_name.toInt).substring(0,4))
              if (m.matches()) {
                "null"
              } else {
                try {
                  val date = new SimpleDateFormat(old_format).parse(aa(fields_name.toInt))
                  val res = new SimpleDateFormat(new_format).format((date))
                 // aa.updated(fields_name.toInt, res).mkString(mark)
                  "success"
                } catch {
                  case ex: Exception => {
                    "null"
                  }
                }
              }
            }).filter(x=>x!="null").count()
          }
          println("格式替换计数完成")

          //////////////////////////////
          if (ss < rules.length - 1) {
            val conf11 = new Configuration()
            val fs = FileSystem.get(conf11)
            if (fs.exists(new Path(hdfs6+"/test/GSTH" + myflat))) {
              fs.delete(new Path(hdfs6+"/test/GSTH" + myflat), true)
            }
            input.saveAsTextFile(hdfs6+"/test/GSTH" + myflat)
            println("格式替换保存路径 :"+hdfs6 +"/test/GSTH"+myflat)

          } else {
            val conf11 = new Configuration()
            val fs = FileSystem.get(conf11)
            if (fs.exists(new Path(outputfile))) {
              fs.delete(new Path(outputfile), true)
            }
            input.coalesce(1, true).saveAsTextFile(outputfile)
            println("格式替换保存路径 :"+outputfile)
          }
        //  sc.stop()
            resnum.toString
        }catch {
          case ex:Exception=>{
            println(time3+" GSTH is fail , the message is :"+ex)
          sc.stop()
            "fail"
          }
        }
  }



}
