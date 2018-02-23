package Mysql

import java.io.{File, FileInputStream}
import java.util.Properties

case class ReadProperties() {
    def get(parameter:String):String={
      val postgprop = new Properties
      //val in = new FileInputStream(new File("d://workspacetest/SparkStatisticsToDS/sparkstatisticsconf.properties"))
      val in = new FileInputStream(new File("/opt/soft/HiveFile-Clean/newclean/cleanjavaconf.properties"))
      postgprop.load(in)
      val res =postgprop.getProperty(parameter)
      res
    }
}
