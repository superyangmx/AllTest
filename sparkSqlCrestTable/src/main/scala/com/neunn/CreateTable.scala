package com.neunn

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONArray, JSONObject}


/**
  * Created by xuzq on 16-8-9.
  */

object CreateTable {

  def main(args: Array[String]): Unit = {

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,S")
    if (args.length != 1) {
      println("Usage: CreateTable <parames>")
      System.exit(1)
    }

    val paramJson = new JSONObject(args(0))
    println(ft.format(new Date()) + " the paramJson is: " + paramJson)

    val flag = paramJson.get("flag")
    val paramArray = paramJson.get("parame").asInstanceOf[JSONArray]

    val conf = new SparkConf().setAppName("CreateTable").set("spark.cores.max", "82") /*.setMaster("local[2]")*/
    val sc = new SparkContext(conf)

    if ("hive".equals(flag)) {
      //获取相关参数
      val dataBaseName = paramArray.get(0).toString
      val tableName = paramArray.get(1).toString
      val outputPath = paramArray.get(2).toString
      val queryId = paramArray.get(3).toString

      println(ft.format(new Date()) + " the dataBaseName, tableName, outputPath, queryId is: %s, %s, %s, %s".format(dataBaseName, tableName, outputPath, queryId))

      //声明一个hiveContext，方便操作hive
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      //使用Hive中的数据库
      sqlContext.sql("use " + dataBaseName)

      //获取table对应的表结构
      //{"fields":[{"name":"vehicleno","comment":"","type":"varchar(500)"},{"name":"vehiclecolor","comment":"","type":"decimal(12,2)"},{"name":"lon","comment":"","type":"string"},{"name":"lat","comment":"","type":"string"},{"name":"speed","comment":"","type":"decimal(12,2)"},{"name":"recordspeed","comment":"","type":"decimal(12,2)"},{"name":"mile","comment":"","type":"decimal(12,2)"},{"name":"recevicetime","comment":"","type":"string"},{"name":"heigh","comment":"","type":"decimal(12,2)"},{"name":"direction","comment":"","type":"decimal(12,2)"},{"name":"state","comment":"","type":"decimal(12,2)"},{"name":"alarm","comment":"","type":"decimal(12,2)"},{"name":"mmlon","comment":"","type":"decimal(12,2)"}]}
      val schemas = sqlContext.sql("desc " + tableName).collect()
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
      val filePath = sqlContext.sql("desc formatted " + tableName).collect()
      var size: Long = 0
      for (eachData <- filePath) {
        if (eachData.get(0).toString.indexOf("Location") != -1) {
          println(ft.format(new Date()) + " the filePath is: " + eachData.get(0).toString)
          val file = eachData.get(1).toString
          println(ft.format(new Date()) + " the filePaht is: " + file)
          //获取table元数据的大小
          val fileSystem = FileSystem.get(sc.hadoopConfiguration)
          //val size = fileSystem.getContentSummary(new Path("hdfs://hdp-neunn-cluster/home/ydhl/hive/warehouse/ydhl_defaultdatabase.db/tg_2k1v_ly_New")).getLength
          size = fileSystem.getContentSummary(new Path(file)).getLength
        }
      }

      //将hive数据存成特定的parquet文件
      sqlContext.sql("select * from " + tableName).write.save(outputPath.toString)
      sqlContext.read.parquet(outputPath.toString).registerTempTable("testTable")

      //获取表的总行数
      val lineNums = sqlContext.sql("select count(*) from testTable").collect()
      val num = lineNums(0).get(0).toString

      //将结果信息插入到数据库中
      val mySqlAction = new MySqlAction("10.2.1.10", "3306", "db_nbds", "nbds_user", "neunbds")
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
      val sqlContext = new SQLContext(sc)

      if (size.toString.toLong <= 1024 * 1024 * 50) {
        println("the mysql table size < 50MB")
        val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://%s:%s/%s".format(host, port, dbName), "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "%s".format(tableName), "user" -> "%s".format(userName), "password" -> "%s".format(passWord))).load()
        jdbcDF.write.save(outputPath)
      }
      else {
        println("the mysql table size > 50MB")
        //设置底限
        val lowerBound = 1
        //设置上限
        val upperBound = 100000000
        //设置分区
        val numPartitions = 100
        val prop = new java.util.Properties
        prop.setProperty("user", userName)
        prop.setProperty("password", passWord)
        println(columnName)

        val jdbcDF = sqlContext.read.jdbc("jdbc:mysql://%s:%s/%s".format(host, port, dbName), tableName, columnName, lowerBound, upperBound, numPartitions, prop)
        jdbcDF.write.save(outputPath)
      }

      //获取表数据的总行数
      sqlContext.read.parquet(outputPath.toString).registerTempTable("testTable")
      val lineNums = sqlContext.sql("select count(*) from testTable").collect()
      val num = lineNums(0).get(0).toString

      //将结果信息插入到数据库中
      val mySqlAction = new MySqlAction("10.2.1.10", "3306", "db_nbds", "nbds_user", "neunbds")
      mySqlAction.modify("UPDATE spark_sql_table set row_info = '%s', line_nums = %s, file_size = %s where auto_id = %s".format(schemaJsonObj.toString, num.toString, size.toString, queryId.toString))
    }
  }
}
