package com.neunn

import org.json.{JSONArray, JSONObject, JSONStringer}


/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val test = "Location:           \thdfs://hdp-neunn-cluster/home/ydhl/hive/warehouse/ydhl_defaultdatabase.db/tg_2k1v_ly_New"

    println(test.split("\t")(1))
    test.split("\t").foreach(println)
  }

}
