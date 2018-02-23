package scalaTextfile

import Mysql.ReadProperties

class Textfile {
  val readProperties = new ReadProperties()
  val hdfs2 = readProperties.get("hdfs")
  def selectALL( ss:Int,rule: String, rules: List[String]): String = {
      var flat=0
if(rule=="41") {
          val ss1 = ss - 1
          val rule1 = rules.apply(ss1)
        if (rule1 == "11") {
            for (i <- rules) {
              if (i == "11") {flat += 1}
            }
          hdfs2+"/test/QK" + flat
        } else if (rule1 == "42") {
            for (i <- rules) {
              if (i == "42") {flat += 1}
            }
          hdfs2+"/test/HLFWGL" + flat
        } else if (rule1 == "12") {
            for (i <- rules) {
              if (i == "12") {flat += 1}
            }
          hdfs2+"/test/KZTC" + flat
        } else if (rule1 == "2") {
            for (i <- rules) {
              if (i == "2") {flat += 1}
            }
          hdfs2+"/test/GSTH" + flat
        } else if(rule1=="33"){
          for(i<-rules){
            if(i =="33"){flat +=1}
          }
          hdfs2+"/test/IDres"+flat
        }else if(rule1=="34"){
          for(i<-rules){
            if(i =="34"){flat +=1}
          }
          hdfs2+"/test/Phoneres"+flat
        }else if (rule1 == "31") {
          hdfs2+"/test/MGCSC"
        } else if (rule1 == "32") {
          hdfs2+"/test/MGCTH"
          }
        else {
            "无调用"
          }
}else if(rule=="31"){
            val ss1 = ss-1
            val rule1=rules.apply(ss1)
        if(rule1=="11"){
            for(i<-rules){
              if(i =="11"){flat +=1}
            }
          hdfs2+"/test/QK"+flat
        }else if(rule1=="12"){
            for(i<-rules){
              if(i =="12"){flat +=1}
            }
          hdfs2+"/test/KZTC"+flat
        }else if(rule1=="42"){
            for(i<-rules){
              if(i =="42"){flat +=1}
            }
          hdfs2+"/test/HLFWGL"+flat
        }else if(rule1=="2"){
            for(i<-rules){
              if(i =="2"){flat +=1}
            }
          hdfs2+"/test/GSTH"+flat
        }else if(rule1=="33"){
          for(i<-rules){
            if(i =="33"){flat +=1}
          }
          hdfs2+"/test/IDres"+flat
        }else if(rule1=="34"){
          for(i<-rules){
            if(i =="34"){flat +=1}
          }
          hdfs2+"/test/Phoneres"+flat
        }else if(rule1=="41"){
          hdfs2+"/test/QC"
         } else if(rule1=="32"){
          hdfs2+"/test/MGCTH"
        }else{"无调用"}
}else if(rule=="32"){
          val ss1 = ss-1
          val rule1=rules.apply(ss1)
        if(rule1=="11"){
            for(i<-rules){
              if(i =="11"){flat +=1}}
          hdfs2+"/test/QK"+flat
        }else if(rule1=="12"){
            for(i<-rules){
              if(i =="12"){flat +=1}}
          hdfs2+"/test/KZTC"+flat
        }else if(rule1=="42"){
            for(i<-rules){
              if(i =="42"){flat +=1}}
          hdfs2+"/test/HLFWGL"+flat
        }else if(rule1=="2"){
            for(i<-rules){
              if(i =="2"){flat +=1}}
          hdfs2+"/test/GSTH"+flat
        }else if(rule1=="33"){
          for(i<-rules){
            if(i =="33"){flat +=1}
          }
          hdfs2+"/test/IDres"+flat
        }else if(rule1=="34"){
          for(i<-rules){
            if(i =="34"){flat +=1}
          }
          hdfs2+"/test/Phoneres"+flat
        }else if(rule1=="31"){
          hdfs2+"/test/MGCSC"
        }else if(rule1=="41"){
          hdfs2+"/test/QC"
        }else{"无调用"}
}else{"方法序号错误"}}


def select( ss:Int,rule: String, rules: List[String],myflat:Int): String = {
    var flat =0
    var front_myflat=myflat-1
if(rule=="42"){
        val ss1 = ss-1
        val rule1=rules.apply(ss1)
    if(rule1=="41"){
      hdfs2+"/test/QC"
    }else if(rule1=="31"){
      hdfs2+"/test/MGCSC"
    }else if(rule1=="32"){
      hdfs2+"/test/MGCTH"
    }else if(rule1=="11"){
          for(i<-rules){
            if(i =="11"){flat +=1}}
      hdfs2+"/test/QK"+flat
    }else if(rule1=="12"){
          for(i<-rules){
            if(i =="12"){flat +=1}}
      hdfs2+"/test/KZTC"+flat
    }else if(rule1=="2"){
          for(i<-rules){
            if(i =="2"){flat +=1}}
      hdfs2+"/test/GSTH"+flat
    }else if(rule1=="33"){
        for(i<-rules){
          if(i =="33"){flat +=1}}
      hdfs2+"/test/IDres"+flat
    }else if(rule1=="34"){
      for(i<-rules){
        if(i =="34"){flat +=1}
      }
      hdfs2+"/test/Phoneres"+flat
    }else if(rule1=="42"){
      hdfs2+"/test/HLFWGL"+front_myflat.toString
    }else{"无调用"}
}else if(rule=="11"){
      val ss1 = ss-1
      val rule1=rules.apply(ss1)
  if(rule1=="41"){
    hdfs2+"/test/QC"
    }else if(rule1=="31"){
    hdfs2+"/test/MGCSC"
    }else if(rule1=="32"){
    hdfs2+"/test/MGCTH"
    }else if(rule1=="42"){
        for(i<-rules){
          if(i =="42"){flat +=1}}
    hdfs2+"/test/HLFWGL"+flat
    }else if(rule1=="12"){
        for(i<-rules){
          if(i =="12"){flat +=1}}
    hdfs2+"/test/KZTC"+flat
    }else if(rule1=="2"){
        for(i<-rules){
          if(i =="2"){flat +=1}}
    hdfs2+"/test/GSTH"+flat
    }else if(rule1=="33"){
      for(i<-rules){
        if(i =="33"){flat +=1}
     }
    hdfs2+"/test/IDres"+flat
  }else if(rule1=="34"){
    for(i<-rules){
      if(i =="34"){flat +=1}
    }
    hdfs2+"/test/Phoneres"+flat
  }else if(rule1=="11"){//自己找自己文件

    hdfs2+"/test/QK"+front_myflat.toString
   }else{"无调用"}

}else if(rule=="12"){
          val ss1 = ss-1
          val rule1=rules.apply(ss1)
    if(rule1=="41"){
      hdfs2+"/test/QC"
    }else if(rule1=="31"){
      hdfs2+"/test/MGCSC"
    }else if(rule1=="32"){
      hdfs2+"/test/MGCTH"
    }else if(rule1=="11"){
            for(i<-rules){
              if(i =="11"){flat +=1}}
      hdfs2+"/test/QK"+flat
    }else if(rule1=="42"){
            for(i<-rules){
              if(i =="42"){flat +=1}}
      hdfs2+"/test/HLFWGL"+flat
    }else if(rule1=="2"){
            for(i<-rules){
              if(i =="2"){flat +=1}}
      hdfs2+"/test/GSTH"+flat
    }else if(rule1=="33"){
      for(i<-rules){
        if(i =="33"){flat +=1}
      }
      hdfs2+"/test/IDres"+flat
    }else if(rule1=="34"){
      for(i<-rules){
        if(i =="34"){flat +=1}
      }
      hdfs2+"/test/Phoneres"+flat
    }else if(rule1=="12"){
      hdfs2+"/test/KZTC"+front_myflat.toString
          }else {"无调用"}
}else if(rule=="2"){
          val ss1 = ss-1
          val rule1=rules.apply(ss1)
    if(rule1=="11"){
            for(i<-rules){
              if(i =="11"){flat +=1}
            }
      hdfs2+"/test/QK"+flat
    }else if(rule1=="12"){
            for(i<-rules){
              if(i =="12"){flat +=1}
            }
      hdfs2+"/test/KZTC"+flat
    }else if(rule1=="31"){
      hdfs2+"/test/MGCSC"
    }else if(rule1=="42"){
            for(i<-rules){
              if(i =="42"){flat +=1}
             }
      hdfs2+"/test/HLFWGL"+flat
    }else if(rule1=="33"){
        for(i<-rules){
          if(i =="33"){flat +=1}
        }
      hdfs2+"/test/IDres"+flat
    }else if(rule1=="34"){
      for(i<-rules){
        if(i =="34"){flat +=1}
      }
      hdfs2+"/test/Phoneres"+flat
    }else if(rule1=="41"){
      hdfs2+"/test/QC"
    }else if(rule1=="32"){
      hdfs2+"/test/MGCTH"
    }else if(rule1=="2"){
      hdfs2+"/test/GSTH"+front_myflat.toString
          }else{"无调用"}
}else if(rule=="33"){
      val ss1 = ss-1
      val rule1=rules.apply(ss1)
      if(rule1=="11"){
        for(i<-rules){
          if(i =="11"){flat +=1}
        }
        hdfs2+"/test/QK"+flat
      }else if(rule1=="12"){
        for(i<-rules){
          if(i =="12"){flat +=1}
        }
        hdfs2+"/test/KZTC"+flat
      }else if(rule1=="31"){
        hdfs2+"/test/MGCSC"
      }else if(rule1=="42"){
        for(i<-rules){
          if(i =="42"){flat +=1}
        }
        hdfs2+"/test/HLFWGL"+flat
      }else if(rule1=="41"){
        hdfs2+"/test/QC"
      }else if(rule1=="32"){
        hdfs2+"/test/MGCTH"
      }else if(rule1=="2"){
        for(i<-rules){
          if(i =="2"){flat +=1}
        }
        hdfs2+"/test/GSTH"+flat
      }else if(rule1=="33"){
        hdfs2+"/test/IDres"+front_myflat.toString
      }else if(rule1=="34"){
        for(i<-rules){
          if(i =="34"){flat +=1}
        }
        hdfs2+"/test/Phoneres"+flat
      }
      else{"无调用"}

}else if(rule=="34"){
        val ss1 = ss-1
        val rule1=rules.apply(ss1)
        if(rule1=="11"){
          for(i<-rules){
            if(i =="11"){flat +=1}
          }
          hdfs2+"/test/QK"+flat
        }else if(rule1=="12"){
          for(i<-rules){
            if(i =="12"){flat +=1}
          }
          hdfs2+"/test/KZTC"+flat
        }else if(rule1=="31"){
          hdfs2+"/test/MGCSC"
        }else if(rule1=="42"){
          for(i<-rules){
            if(i =="42"){flat +=1}
          }
          hdfs2+"/test/HLFWGL"+flat
        }else if(rule1=="41"){
          hdfs2+"/test/QC"
        }else if(rule1=="32"){
          hdfs2+"/test/MGCTH"
        }else if(rule1=="2"){
          for(i<-rules){
            if(i =="2"){flat +=1}
          }
          hdfs2+"/test/GSTH"+flat
        }else if(rule1=="33"){
          for(i<-rules){
            if(i =="33"){flat +=1}
          }
          hdfs2+"/test/IDres"+flat

        }else if(rule1=="34"){
          hdfs2+"/test/Phoneres"+front_myflat.toString
        }
        else{"无调用"}

}else{"方法序号错误"}
  }




}
