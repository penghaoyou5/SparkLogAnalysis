package Online.utils

object ClientInfoUtil {

  val agentBrowerOsDict = scala.collection.mutable.Map[String,(String,String)]()
  val defaultBrowerOs = ("Other","Other")


  def main(args: Array[String]): Unit = {
    var agentStr = "Mozilla/5.0 Gecko/20100115 Firefox/3.6"

    agentStr = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.2; WOW64; Trident/7.0; .NET4.0E; .NET4.0C; .NET CLR 3.5.30729; .NET CLR 2.0.50727; .NET CLR 3.0.30729)"

    val result = getBrowserOs(agentStr)
    println(result)
  }

  def getBrowserOs(agent:String):(String,String) = {
    val result = agentBrowerOsDict.getOrElse(agent,("",""))
    if(result != ("","")){
      result
    }else{
      var resultCurrent = defaultBrowerOs
      try {
        resultCurrent = getBrowserOsNormal(agent)
      }catch {
        case  ex: Exception => {
          //        resultMap = Map(("uriHost",""),("timestampStr","197001010000"))
        }
      }
      agentBrowerOsDict(agent) = resultCurrent
      resultCurrent
    }

  }



  def getBrowserOsNormal(agent:String):(String,String) = {
    val info = agent.toUpperCase()
    val strInfo = info.substring(info.indexOf("(") + 1,info.indexOf(")") - 1).split(";")
    if ((info.indexOf("MSIE")) > -1) {
      (strInfo(1).trim,strInfo(2).trim)
    } else {
      val str = info.split(" ");
      if (info.indexOf("NAVIGATOR") < 0 && info.indexOf("FIREFOX") > -1) {
        (str(str.length - 1).trim,strInfo(2).trim)
      } else if ((info.indexOf("OPERA")) > -1) {
        (str(0).trim,strInfo(0).trim)
      } else if (info.indexOf("CHROME") < 0
        && info.indexOf("SAFARI") > -1) {
        (str(str.length - 1).trim,strInfo(2).trim)
      } else if (info.indexOf("CHROME") > -1) {
        (str(str.length - 2).trim,strInfo(2).trim)
      } else if (info.indexOf("NAVIGATOR") > -1) {
        (str(str.length - 1).trim,strInfo(2).trim)
      } else {
        ("Other","Other")
      }
    }

  }


}
