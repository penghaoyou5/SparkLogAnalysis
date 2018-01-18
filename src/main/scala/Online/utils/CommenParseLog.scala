package Online.utils

import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.util.matching.Regex


object CommenParseLog {

  val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val jsonParser = new JSONParser()

  def main(args: Array[String]): Unit = {
    val kafka_log = "{\"source\":\"/data/cache1/filbeat_conf/logsdir/CHN-JI-3-3gD.dhdown.5211game.com-03.log\",\"type\":\"log\",\"@version\":\"1\",\"@timestamp\":\"2018-01-12T07:38:12.228Z\",\"offset\":81317701,\"prospector\":{\"type\":\"log\"},\"host\":\"BGP-SM-4-3gf\",\"message\":\"1512270367.090  92542 116.29.4.214 TCP_HIT/200 153420062 GET http://dhdown.5211game.com/download/360/dl/zjhl_setup360wan_all_1.0.3.19_ef.exe  - NONE/- application/octet-stream \\\"http://dhol.wan.360.cn/sindex.html\\\" \\\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36\\\" \\\"-\\\"\",\"tags\":[\"beats_input_codec_plain_applied\"],\"beat\":{\"name\":\"BGP-SM-4-3gf\",\"version\":\"6.1.1\",\"hostname\":\"BGP-SM-4-3gf\"}}"

    val resultMap =  parseLogToTupNormal(kafka_log)
    println(resultMap)

    var resultMapTes:Map[String,String] =  Map[String,String]()
    println(resultMapTes.nonEmpty)

  }



  /**
    *
    * @param kafka_log
    *    1512319851.385    117 120.221.134.92 TCP_REFRESH_HIT/304 199 GET http://dhdown.5211game.com/download/EF_patch_1.0.2.10-1.0.2.11.exe  - DIRECT/122.228.246.78 - "-" "Mozilla/5.0 Gecko/20100115 Firefox/3.6" "-"
    * @return
    *         map 获取类型直接get 为Option
    *
    */
  def parseLogToTup (kafka_log: String):Map[String,String] =   {

    var resultMap =  Map[String,String]()

    try {
      resultMap = parseLogToTupNormal(kafka_log)
    }catch {
      case  ex: Exception => {
//        resultMap = Map(("uriHost",""),("timestampStr","197001010000"))
      }
    }
    resultMap
  }


  /**
    *
    * @param kafka_log
    * @return  Map(clientIp -> 116.29.4.214, repSize -> 153420062, timestampStr -> 201712031106, mime -> application/octet-stream, agentCheck -> Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36, uriHost -> dhdown.5211game.com)
    *
    * */
  def parseLogToTupNormal (kafka_log: String):Map[String,String]=   {
    val jsonObj: JSONObject = jsonParser.parse(kafka_log).asInstanceOf[JSONObject]

    /**
      * message原始日志
      * 1512270367.090  92542 116.29.4.214 TCP_HIT/200 153420062 GET http://dhdown.5211game.com/download/360/dl/zjhl_setup360wan_all_1.0.3.19_ef.exe  - NONE/- application/octet-stream "http://dhol.wan.360.cn/sindex.html" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36" "-"
      * 按照空格分割代表
      * timestamp   reqtime  clientIP  squidCache  repsize  reqMethod  requestURL  username requestOriginSite mime referer agentCheck
      */
    val message = jsonObj.getAsString("message")
    val messageArray = message.split(" ").filterNot(_.isEmpty)

    //时间戳转换  1512270367.090   ->  201712031106
    val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(messageArray(0).replace(".", ""))))

    val clientIp = messageArray(2)

    val repSize = messageArray(4)

    val requestUrl = messageArray(6)
    val requestUrlArray = requestUrl.split("/").filterNot(_.isEmpty)
    val uriHost = requestUrlArray(1)

    val mime = messageArray(9)


    val agentCheck = message.split("\"").filterNot(" "==_)(2)

    val userId = WeblukerSqlQueryDict.domainUserDict.getOrElse(uriHost,"")

//    (timestampStr,clientIp,repSize,uriHost,mime)
    Map(("uriHost",uriHost),("timestampStr",timestampStr),("clientIp",clientIp),("repSize",repSize),("mime",mime),("agentCheck",agentCheck),("userId",userId))
  }

}
