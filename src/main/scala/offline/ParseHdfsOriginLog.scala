package offline

import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import parlog.TotalIPCount.parseLogRight

object ParseHdfsOriginLog {

  def main(args: Array[String]): Unit = {
    val hdfsOriginLog = "BGP-SM-4-3gf 1512401838.905    219 49.119.164.208 TCP_REFRESH_MISS/504 1303 GET http://dhdown.5211game.com/download/360/dl/zjhl_setup360wan_all_1.0.3.19_ef.exe?channel=617490001  - DIRECT/58.216.31.37 text/html \"http://dhol.wan.360.cn/sindex.html?channel=617490001&from=217600000023&placeid=\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.2; WOW64; Trident/7.0; .NET4.0E; .NET4.0C; .NET CLR 3.5.30729; .NET CLR 2.0.50727; .NET CLR 3.0.30729)\" \"-\""
    val result = parseHdfsOriginLog(hdfsOriginLog)
    println(result)
  }

  /**
    *
    * @param hdfsOriginLog
    * @return
    */
  def parseHdfsOriginLog(hdfsOriginLog:String): ((String,String),Int) ={
    var nomal_result: ((String,String),Int) =  (("hdfsunknowhost", "197001010101"), 0)
    try {
      nomal_result = parseLogNormal(hdfsOriginLog)
    }catch {
      case  ex: Exception => {
        nomal_result =  ((hdfsOriginLog, "197001010101"), 0)  //这一句后加的记录错误日志吧
      }
    }
    nomal_result
  }


  def parseLogNormal(hdfsOriginLog:String): ((String,String),Int) ={
    val messageArray = hdfsOriginLog.split(" ").filter(!"   ".contains(_))
    val timestamp = messageArray(1)
    val timestampS: java.lang.String = timestamp.replace(".", "")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(timestampS)))
    val requestUrl = messageArray(7)
    val requestUrlArray = requestUrl.split("/").filter(!"   ".contains(_))
    val uriHost = requestUrlArray(1)
    ((uriHost, timestampStr), 1)
  }







}
