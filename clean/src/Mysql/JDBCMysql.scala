package Mysql

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

class JDBCMysql {
  classOf[com.mysql.jdbc.Driver]
  // 创建数据库连接
/* val filePath =System.getProperty("user.dir")//设定为jar包的绝对路径 在IDE中运行时为project的绝对路径

  val ipstream = new BufferedInputStream(new FileInputStream(filePath+"/conf/config.properties"))
  postgprop.load(ipstream)
  val dbProp =  postgprop.getProperty("URL")
    print(dbProp)*/


  val readProperties = new ReadProperties()
  val url1 = readProperties.get("URL")

  private val conn = DriverManager.getConnection(url1)
  private val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //设置日期格式
  val time = df.format(new Date())
  // new Date()为获取当前系统时间
  def addCheckPoint(jobtype: String,point: String,auto_id:String): Unit = {
    try {

      val Sql = jobtype match {
        case "run" => "UPDATE hive_dataclean_history SET job_status = '" + point + "' WHERE auto_id ="+auto_id
        case "success" => "UPDATE hive_dataclean_history SET job_status = '" + point + "', finish_time = '" + time + "' WHERE auto_id="+auto_id
      }


      println("Processing: " + Sql)
      val prep = conn.prepareStatement(Sql) //载入sql(更新数据库，更新table表，把id_name字段=id的数据的state字段内容变成point
      prep.executeUpdate() //执行sql
    }catch{
      case ex:Exception=>{
        print("JDBCMysql更新失败 the message is :"+time+ex)
      }
    }
  }

  def addresult(rule:String,auto_id:String):Unit={
    try{
      val Sql = "UPDATE hive_dataclean_history SET result_header ='"+rule+"' WHERE auto_id ="+auto_id
      println(Sql)
      val prep =conn.prepareStatement(Sql)
      prep.executeUpdate()
    }catch {
      case ex:Exception=>{
        print("结果插入表中失败，the message is :"+ time+" "+ex)
      }
    }
  }


  //conn.close()
}