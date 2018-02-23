package scalamain

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import Mysql.{JDBCMysql, ReadProperties}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control._
import scalaimpl.{AbnormalDataDelet, CleanFIle, IDandPhone, SensitiveWordDelet}

object MyClean extends IDandPhone with SensitiveWordDelet with AbnormalDataDelet  with CleanFIle {
  def main(args:Array[String]): Unit = {
    val df5 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time5 = df5.format(new Date())
    println(time5+" 进入scala清洗方法")
    val all_rule=new ArrayBuffer[String]()
    val QCtmp = new util.ArrayList[String]()
    val MGCSCtmp = new util.ArrayList[String]()
    val MGCTHtmp = new util.ArrayList[String]()
    val HLFWGLtmp = new util.ArrayList[String]()
    val QKtmp = new util.ArrayList[String]()
    val KZTCtmp = new util.ArrayList[String]()
    val GSTHtmp = new util.ArrayList[String]()
    val IDrestmp = new util.ArrayList[String]()
    val Phonerestmp = new util.ArrayList[String]()

    val conf = new SparkConf()
      //.setMaster("spark://data1.cshdp.com:7077")
      //.setMaster("local")
      .setAppName("sparkclean")
      .set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
   println("SparkContext获取")
   // val wm=args(0).replace("无码无码","\"").replace("高清高清",",").replace("老司机"," ")
    println("args(0) is :"+args(0))
    val json=JSON.parseObject(args(0))
    println("json is :"+json)
    val auto_id=json.get("auto_id").toString
    val all = json.getJSONArray("all_rule").toArray()
    val fields=json.getJSONArray("fields_rule")
    val fields1=json.getJSONArray("fields_rule").size()
   // val start_hdpuser=json.get("start_hdpuser").toString
    val start_db_name=json.get("start_db_name").toString
    val start_table_name= json.get("start_table_name").toString
   // val end_hdpuser=json.get("end_hdpuser").toString
    val user = json.get("user").toString
    val mark = json.get("table_delim").toString
    val end_db_name=json.get("end_db_name").toString
    val end_table_name=json.get("end_table_name").toString
   println("Json解析完成，the json is : "+json)
    val readProperties = new ReadProperties()
    val hdfs1 = readProperties.get("hdfs")
    println("hdfs1 = :"+hdfs1)
//    val files =hdfs1+"/home/"+start_hdpuser+"/hive/warehouse/"+start_db_name+".db/"+start_table_name+"/"
//    val outputfile =hdfs1+"/home/"+end_hdpuser+"/hive/warehouse/"+end_db_name+".db/"+end_table_name
    val files =hdfs1+"/home/"+user+"/hive/warehouse/"+start_db_name+".db/"+start_table_name+"/"
    val outputfile =hdfs1+"/home/"+user+"/hive/warehouse/"+end_db_name+".db/"+end_table_name
   println("the file path is :"+files+" "+" the outputfile path is : "+outputfile)

    if(json.getJSONArray("all_rule") !="[]"&& json.getJSONArray("fields_rule") !="[]") {
          for (i <- all) {
            all_rule += i.toString
          }
          for (j <- 0 to fields1 - 1) {
            all_rule += fields.getJSONObject(j).get("rule").toString
          }
    } else if(json.getJSONArray("all_rule") =="[]"&&json.getJSONArray("fields_rule") !="[]") {
          for (j <- 0 to fields1 - 1) {
            all_rule += fields.getJSONObject(j).get("rule").toString
          }
    }else if(json.getJSONArray("all_rule") !="[]"&&json.getJSONArray("fields_rule") =="[]"){
          for (i <- all) {
            all_rule += i.toString
          }
    }else{print("无方法序号")}
    val rules=all_rule.toList
    val length = rules.length.toDouble
    val numaa=1/length

    val Mysql = new JDBCMysql
    val loop = new Breaks
    val outloop=new Breaks
    var field_name=""
    var fill_content=""
    var start_reasonable=""
    var end_reasonable=""
    var old_format=""
    var new_format=""
    var returnjudgment=""
    var KZTCflat=0
    var QKflat =0
    var HLFWGLflat=0
    var GSTHflat=0
    var IDresflat=0
    var Phoneresflat=0
    var aaa=0
    println("rules is :"+rules)
    print("开始清洗")
try {
  outloop.breakable {
    for (ii <- rules) {
      if (ii.toInt == 41) { //去重41
        aaa = rules.indexOf("41")
        if ((numaa * (aaa + 1)).formatted("%.2f").toString != "1.00") {
          returnjudgment = cleanfile(sc,rules, files, outputfile)
          if (returnjudgment != "fail") {
            QCtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + 1)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = cleanfile(sc,rules, files, outputfile)
          if (returnjudgment != "fail") {
            QCtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = cleanfile(sc,rules, files, outputfile)
          if (returnjudgment != "fail") {
            QCtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 31) {
        //敏感词删除31
        aaa = rules.indexOf("31")
        if ((numaa * (aaa + 1)).formatted("%.2f").toString != "1.00") {
          returnjudgment = sensitivewordelet(sc,rules, files, outputfile, json.get("sensitive_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCSCtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + 1)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = sensitivewordelet(sc,rules, files, outputfile, json.get("sensitive_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCSCtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = sensitivewordelet(sc,rules, files, outputfile, json.get("sensitive_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCSCtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 32) {
        //敏感词替换32
        aaa = rules.indexOf("32")
        if ((numaa * (aaa + 1)).formatted("%.2f").toString != "1.00") {
          returnjudgment = sensitivewordreplace(sc,rules, files, outputfile, json.get("sensitive_content").toString, json.get("replace_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + 1)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }

        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = sensitivewordreplace(sc,rules, files, outputfile, json.get("sensitive_content").toString, json.get("replace_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }

        if ((numaa * (aaa + 1)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = sensitivewordreplace(sc,rules, files, outputfile, json.get("sensitive_content").toString, json.get("replace_content").toString,mark)
          if (returnjudgment != "fail") {
            MGCTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 11) {
        //带空值删除11
        QKflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "11") {
              var jQK = j + QKflat - 1
              field_name = fields.getJSONObject(jQK).get("index").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("11")
        if ((numaa * (aaa + QKflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = cleannull(sc,rules, files, outputfile, field_name, QKflat,mark)
          if (returnjudgment != "fail") {
            QKtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + QKflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + QKflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = cleannull(sc,rules, files, outputfile, field_name, QKflat,mark)
          if (returnjudgment != "fail") {
            QKtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + QKflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = cleannull(sc,rules, files, outputfile, field_name, QKflat,mark)
          if (returnjudgment != "fail") {
            QKtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 12) {
        //空值填充12
        KZTCflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "12") {
              var jKZTC = j + KZTCflat - 1
              field_name = fields.getJSONObject(jKZTC).get("index").toString
              fill_content = fields.getJSONObject(jKZTC).get("fill_content").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("12")
        if ((numaa * (aaa + KZTCflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = filldata(sc,rules, files, outputfile, field_name, fill_content, KZTCflat,mark)
          if (returnjudgment != "fail") {
            KZTCtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + KZTCflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + KZTCflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = filldata(sc,rules, files, outputfile, field_name, fill_content, KZTCflat,mark)
          if (returnjudgment != "fail")
            KZTCtmp.add(returnjudgment)
          Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          if (returnjudgment == "fail")
            Mysql.addCheckPoint("run", "FAIL", auto_id)
          outloop.break()
        }
        if ((numaa * (aaa + KZTCflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = filldata(sc,rules, files, outputfile, field_name, fill_content, KZTCflat,mark)
          if (returnjudgment != "fail") {
            KZTCtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 42) {
        //根据某列按范围过滤（不符合范围的数据也保存42
        HLFWGLflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "42") {
              var jHLFWGLflat = j + HLFWGLflat - 1
              field_name = fields.getJSONObject(jHLFWGLflat).get("index").toString
              start_reasonable = fields.getJSONObject(jHLFWGLflat).get("start_reasonable").toString
              end_reasonable = fields.getJSONObject(jHLFWGLflat).get("end_reasonable").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("42")
        if ((numaa * (aaa + HLFWGLflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = abnormaldatadelet(sc,rules, files, outputfile, field_name.toInt, start_reasonable.toInt, end_reasonable.toInt, HLFWGLflat,mark)
          if (returnjudgment != "fail") {
            HLFWGLtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + HLFWGLflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + HLFWGLflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = abnormaldatadelet(sc,rules, files, outputfile, field_name.toInt, start_reasonable.toInt, end_reasonable.toInt, HLFWGLflat,mark)
          if (returnjudgment != "fail") {
            HLFWGLtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + HLFWGLflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = abnormaldatadelet(sc,rules, files, outputfile, field_name.toInt, start_reasonable.toInt, end_reasonable.toInt, HLFWGLflat,mark)
          if (returnjudgment != "fail") {
            HLFWGLtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 2) {
        //日期格式化(简单版)
        GSTHflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "2") {
              var jGSTHflat = j + GSTHflat - 1
              field_name = fields.getJSONObject(jGSTHflat).get("index").toString
              old_format = fields.getJSONObject(jGSTHflat).get("old_format").toString
              new_format = fields.getJSONObject(jGSTHflat).get("new_format").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("2")
        if ((numaa * (aaa + GSTHflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = dateformat(sc,rules, files, outputfile, field_name, old_format, new_format, GSTHflat,mark)
          if (returnjudgment != "fail") {
            GSTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + GSTHflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + GSTHflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = dateformat(sc,rules, files, outputfile, field_name, old_format, new_format, GSTHflat,mark)
          if (returnjudgment != "fail") {
            GSTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + GSTHflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = dateformat(sc,rules, files, outputfile, field_name, old_format, new_format, GSTHflat,mark)
          if (returnjudgment != "fail") {
            GSTHtmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 33) {
        //身份证号加密
        IDresflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "33") {
              var jIDresflat = j + IDresflat - 1
              field_name = fields.getJSONObject(jIDresflat).get("index").toString
              //start_reasonable = fields.getJSONObject(jIDresflat).get("old_format").toString
              //end_reasonable = fields.getJSONObject(jIDresflat).get("new_format").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("33")
        if ((numaa * (aaa + IDresflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = idsecret(sc,rules, files, outputfile, field_name, IDresflat,mark)
          if (returnjudgment != "fail") {
            IDrestmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + IDresflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + IDresflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = idsecret(sc,rules, files, outputfile, field_name, IDresflat,mark)
          if (returnjudgment != "fail") {
            IDrestmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + IDresflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = idsecret(sc,rules, files, outputfile, field_name, IDresflat,mark)
          if (returnjudgment != "fail") {
            IDrestmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else if (ii.toInt == 34) {
        //手机号码加密
        Phoneresflat += 1
        loop.breakable {
          for (j <- 0 to fields1 - 1) {
            if (fields.getJSONObject(j).get("rule").toString == "34") {
              var jPhoneresflat = j + Phoneresflat - 1
              field_name = fields.getJSONObject(jPhoneresflat).get("index").toString
              //start_reasonable = fields.getJSONObject(jIDresflat).get("old_format").toString
              //end_reasonable = fields.getJSONObject(jIDresflat).get("new_format").toString
              loop.break()
            }
          }
        }
        aaa = rules.indexOf("34")
        if ((numaa * (aaa + Phoneresflat)).formatted("%.2f").toString != "1.00") {
          returnjudgment = phonesecret(sc,rules, files, outputfile, field_name, Phoneresflat,mark)
          if (returnjudgment != "fail") {
            Phonerestmp.add(returnjudgment)
            Mysql.addCheckPoint("run", (numaa * (aaa + Phoneresflat)).formatted("%.2f").toString, auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + Phoneresflat)).formatted("%.2f").toString == "1.00" && length!=1.0) {
          returnjudgment = phonesecret(sc,rules, files, outputfile, field_name, Phoneresflat,mark)
          if (returnjudgment != "fail") {
            Phonerestmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
        if ((numaa * (aaa + Phoneresflat)).formatted("%.2f").toString == "1.00" && length == 1.0) {
          Mysql.addCheckPoint("run", "0.50", auto_id)
          returnjudgment = phonesecret(sc,rules, files, outputfile, field_name, Phoneresflat,mark)
          if (returnjudgment != "fail") {
            Phonerestmp.add(returnjudgment)
            Mysql.addCheckPoint("success", "SUCCESS", auto_id)
          }
          if (returnjudgment == "fail") {
            Mysql.addCheckPoint("run", "FAIL", auto_id)
            outloop.break()
          }
        }
      } else {
        print("没有应用")
      }
    }
  }
  val resultjson = new JSONObject()
  //resultjson.put("rulenum", ruletmp)
  if(QCtmp.size()!=0){resultjson.put("QC",QCtmp)}
  if(MGCSCtmp.size()!=0){resultjson.put("MGCSC",MGCSCtmp)}
  if(MGCTHtmp.size()!=0){resultjson.put("MGCTH",MGCTHtmp)}
  if(HLFWGLtmp.size()!=0){resultjson.put("HLFWGL",HLFWGLtmp)}
  if(QKtmp.size()!=0){resultjson.put("QK",QKtmp)}
  if(KZTCtmp.size()!=0){resultjson.put("KZTC",KZTCtmp)}
  if(GSTHtmp.size()!=0){resultjson.put("GSTH",GSTHtmp)}
  if(IDrestmp.size()!=0){resultjson.put("IDCARD",IDrestmp)}
  if(Phonerestmp.size()!=0){resultjson.put("PHONE",Phonerestmp)}
  Mysql.addresult(resultjson.toString,auto_id)
  sc.stop()
  println("the clean is SUCCESS")
   //resultjson.toString
}catch {
  case ex: Exception => {
    print("the clean is fail ,massage is :"+time5+ex)
    sc.stop()
    "fail"
  }
}
  }
}