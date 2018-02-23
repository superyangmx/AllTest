package com.neunn

import java.sql.DriverManager
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuzq on 16-8-9.
  */
class MySqlAction(val host:String, val port:String, val dbName:String, val userName:String, val passwd:String) {

  classOf[com.mysql.jdbc.Driver]
  val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,S")

  def getConnection(host:String, port:String, dbName:String, userName:String, passwd:String): Connection = {
    val connection = DriverManager.getConnection("JDBC:mysql://%s:%s/%s".format(host, port, dbName), userName, passwd)
    connection
  }

  def close(conn: Connection): Unit ={
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }catch {
      case ex: Exception =>{
        ex.printStackTrace()
      }
    }
  }

  def query(sqlString: String): ArrayBuffer[mutable.HashMap[String, Any]]={
    println(ft.format(new Date()) + " the sqlString is: " + sqlString)
    val conn = getConnection(host, port, dbName, userName, passwd)

    try{
      val pstm = conn.prepareStatement(sqlString)

      val rs = pstm.executeQuery()

      val rsmd = rs.getMetaData()
      val size = rsmd.getColumnCount()
      val buffer = new ArrayBuffer[mutable.HashMap[String, Any]]()

      while(rs.next()){
        val map = mutable.HashMap[String, Any]()
        for (i <- 1 to size){
          map += (rsmd.getColumnLabel(i) -> rs.getString(i))
        }
        buffer.append(map)
      }
      buffer
    }
    finally {
      conn.close()
    }
  }

  def modify(sqlString: String): Unit ={
    println(ft.format(new Date()) + " the sqlString is: " + sqlString)
    val conn = getConnection(host, port, dbName, userName, passwd)
    try{
      val pstm = conn.prepareStatement(sqlString)
      pstm.executeUpdate()
    }catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }finally{
      conn.close()
    }
  }

  def add(sqlString :String): Unit ={
    println(ft.format(new Date()) + " the sqlString is: " + sqlString)
    val conn = getConnection(host, port, dbName, userName, passwd)
    try{
      val pstm = conn.prepareStatement(sqlString)
      pstm.executeUpdate()
    }catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }finally {
      conn.close()
    }
  }
}
