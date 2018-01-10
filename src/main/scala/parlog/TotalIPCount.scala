package parlog

import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

object TotalIPCount {


  def parseLog (kafka_log_tup: Tuple2[String, String] ):((String,String),Int) =   {
    var nomal_result: ((String,String),Int) =  (("unknowhost", "201701010101"), 0)
    try {
      nomal_result = parseLogRight(kafka_log_tup)
    }catch {
      case  ex: Exception => {
        println("error"+kafka_log_tup._2)
        nomal_result =  ((kafka_log_tup._2, "197001010101"), 0)  //这一句后加的记录错误日志吧
      }
    }
    nomal_result
  }



  def parseLogRight (kafka_log_tup: Tuple2[String, String] ):((String,String),Int) =   {
    val jsonParser = new JSONParser()
    val kafka_log = kafka_log_tup._2
    val jsonObj: JSONObject = jsonParser.parse(kafka_log).asInstanceOf[JSONObject]
    val message = jsonObj.getAsString("message")
    //      message
    val messageArray = message.split(" ").filter(!"   ".contains(_))
//      println(messageArray.toBuffer)
      val timestamp = messageArray(0)

      //    val timestampS:java.lang.String = "1514350489.468".replace(".","")
      val timestampS: java.lang.String = timestamp.replace(".", "")
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
      //      DateTimeFormat.forPattern("").parseMillis(timestamp)

      val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(timestampS)))
//      println(timestampStr)

      val requestUrl = messageArray(6)
      val requestUrlArray = requestUrl.split("/").filter(!"   ".contains(_))
//      println(requestUrlArray.toBuffer)
      val uriHost = requestUrlArray(1)
//      val reqSize = messageArray(4).toDouble

      ((uriHost, timestampStr), 1)
    }
}
