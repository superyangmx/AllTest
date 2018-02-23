package com.neunn

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.json.{JSONArray, JSONObject}


/**
  * Created by xuzq on 16-8-9.
  */

object CreateTableNew {

  def main(args: Array[String]): Unit = {

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,S")
    if (args.length != 2) {
      println("Usage: CreateTable <parames> <confPath>")
      System.exit(1)
    }

    val paramJson = new JSONObject(args(0))
    println(ft.format(new Date()) + " the paramJson is: " + paramJson)

    val flag = paramJson.get("flag")
    val paramArray = paramJson.get("parame").asInstanceOf[JSONArray]
    val readProperties = new ReadProperties(args(1))
    val db_host = readProperties.getStringProperty("db_host")
    val db_port = readProperties.getStringProperty("db_port")
    val db_name = readProperties.getStringProperty("db_name")
    val db_userName = readProperties.getStringProperty("db_userName")
    val db_passWord = readProperties.getStringProperty("db_passWord")

    println(ft.format(new Date()) + " the db_host is: " + db_host)

    val mySqlAction = new MySqlAction(db_host, db_port, db_name, db_userName, db_passWord)

    val spark = SparkSession.builder().appName("CreateTabale").config("spark.cores.max", "82").enableHiveSupport().getOrCreate()
    import spark.implicits._
    
    try{
      if ("hive".equals(flag)) {
        //获取相关参数
        val dataBaseName = paramArray.get(0).toString
        val tableName = paramArray.get(1).toString
        val outputPath = paramArray.get(2).toString
        val queryId = paramArray.get(3).toString

        println(ft.format(new Date()) + " the dataBaseName, tableName, outputPath, queryId is: %s, %s, %s, %s".format(dataBaseName, tableName, outputPath, queryId))

        //val spark = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
        spark.sql("use " + dataBaseName)

        val schemas = spark.sql("desc " + tableName).collect()
        val schemaJsonArray = new JSONArray()
        val schemaJsonObj = new JSONObject()
        var count = 0
        for (eachSC <- schemas) {
          val tempJsonObj = new JSONObject()
          tempJsonObj.put("name", eachSC.get(0).toString).put("type", eachSC.get(1).toString).put("comment", eachSC.get(2).toString)
          schemaJsonArray.put(count, tempJsonObj)
          count += 1
        }
        schemaJsonObj.put("fields", schemaJsonArray)
        println(ft.format(new Date()) + " the schema is: " + schemaJsonObj.toString)

        //获取hive表对应的数据路径
        val filePath = spark.sql("desc formatted " + tableName).collect()
        var size: Long = 0
        for (eachData <- filePath) {
          if (eachData.get(0).toString.indexOf("Location") != -1) {
            println(ft.format(new Date()) + " the filePath is: " + eachData.get(0).toString)
            val file = eachData.get(1).toString
            println(ft.format(new Date()) + " the filePaht is: " + file)
            //获取table元数据的大小
            val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration) //val size = fileSystem.getContentSummary(new Path("hdfs://hdp-neunn-cluster/home/ydhl/hive/warehouse/ydhl_defaultdatabase.db/tg_2k1v_ly_New")).getLength
            size = fileSystem.getContentSummary(new Path(file)).getLength
          }
        }

        //将hive数据存成特定的parquet文件
        spark.sql("select * from " + tableName).write.save(outputPath.toString)
        spark.read.parquet(outputPath.toString).registerTempTable("testTable")

        //获取表的总行数
        val lineNums = spark.sql("select count(*) from testTable").collect()
        val num = lineNums(0).get(0).toString

        //将结果信息插入到数据库中
        mySqlAction.modify("UPDATE spark_sql_table set row_info = '%s', line_nums = %s, file_size = %s where auto_id = %s".format(schemaJsonObj.toString, num.toString, size.toString, queryId.toString))
      } else if ("mysql".equals(flag)) {

        //获取相关参数
        val host = paramArray.get(0).toString
        val port = paramArray.get(1).toString
        val dbName = paramArray.get(2).toString
        val userName = paramArray.get(3).toString
        val passWord = paramArray.get(4).toString
        val tableName = paramArray.get(5).toString
        val outputPath = paramArray.get(6).toString
        val queryId = paramArray.get(7).toString

        //声明一个原数据库的句柄
        val sqlServer = new MySqlAction(host, port, dbName, userName, passWord)

        //获取表的结构信息
        val schemas = sqlServer.query("desc " + tableName)
        val schemaJsonArray = new JSONArray()
        val schemaJsonObj = new JSONObject()
        var count = 0

        var columnName = ""
        var flag = 0

        for (eachSC <- schemas) {
          val tempJsonObj = new JSONObject()
          val columnType = eachSC.get("Type").get
          tempJsonObj.put("name", eachSC.get("Field").get).put("type", columnType).put("comment", "")
          if (flag == 0 && columnType.toString.indexOf("int") != -1) {
            columnName = eachSC.get("Field").get.toString
            flag = 1
          }
          schemaJsonArray.put(count, tempJsonObj)
          count += 1
        }
        schemaJsonObj.put("fields", schemaJsonArray)

        println(schemaJsonObj.toString())

        //获取表数据的大小
        val tableSize = sqlServer.query("select data_length from information_schema.tables where table_name = '%s'".format(tableName))
        val size = tableSize.apply(0).get("data_length").get
        println(size)

        //获取数据并保存成parquet

        if (size.toString.toLong <= 1024 * 1024 * 1024 * 2.0) {
          println("the mysql table size < 2GB")
          val jdbcDF = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://%s:%s/%s".format(host, port, dbName), "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "%s".format(tableName), "user" -> "%s".format(userName), "password" -> "%s".format(passWord))).load()
          jdbcDF.write.save(outputPath)

          //获取表数据的总行数
          spark.read.parquet(outputPath.toString).registerTempTable("testTable")
          val lineNums = spark.sql("select count(*) from testTable").collect()
          val num = lineNums(0).get(0).toString

          //将结果信息插入到数据库中
          mySqlAction.modify("UPDATE spark_sql_table set row_info = '%s', line_nums = %s, file_size = %s where auto_id = %s".format(schemaJsonObj.toString, num.toString, size.toString, queryId.toString))
        }
        else{
          throw  new Exception("the mysql table size > 2GB")
        }
      }
    }
    finally{
      spark.stop()
    }
  }
}
