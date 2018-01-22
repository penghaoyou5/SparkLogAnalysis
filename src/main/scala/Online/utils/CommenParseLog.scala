package Online.utils

import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.util.matching.Regex


object CommenParseLog {

  val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val jsonParser = new JSONParser()

  def main(args: Array[String]): Unit = {
    var kafka_log = "{\"source\":\"/data/cache1/filbeat_conf/logsdir/CHN-JI-3-3gD.dhdown.5211game.com-03.log\",\"type\":\"log\",\"@version\":\"1\",\"@timestamp\":\"2018-01-12T07:38:12.228Z\",\"offset\":81317701,\"prospector\":{\"type\":\"log\"},\"host\":\"BGP-SM-4-3gf\",\"message\":\"1512270367.090  92542 116.29.4.214 TCP_HIT/200 153420062 GET http://dhdown.5211game.com/download/360/dl/zjhl_setup360wan_all_1.0.3.19_ef.exe  - NONE/- application/octet-stream \\\"http://dhol.wan.360.cn/sindex.html\\\" \\\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36\\\" \\\"-\\\"\",\"tags\":[\"beats_input_codec_plain_applied\"],\"beat\":{\"name\":\"BGP-SM-4-3gf\",\"version\":\"6.1.1\",\"hostname\":\"BGP-SM-4-3gf\"}}"

    kafka_log = "{\"message\":\"1516597252.198      8 117.136.56.139 TCP_IMS_HIT/304 218 GET http://cdn.kkapp.com/project/kkmessage/json/c_28_1510539734079.json  - NONE/- application/json \\\"-\\\" \\\"Dalvik/1.6.0 (Linux; U; Android 4.4.4; Konka Android TV 638 Build/KTU84P)\\\" \\\"-\\\"\",\"@version\":\"1\",\"@timestamp\":\"2018-01-22T05:00:56.403Z\",\"type\":\"fc_access\",\"file\":\"/data/proclog/log/squid/access.log\",\"host\":\"CMN-HB-1-3gH\",\"offset\":\"468182\"}"

    kafka_log = "{\"message\":\"1516603541.055     23 211.138.88.78 TCP_HIT/200 11255 GET http://m.88yangsheng.com/css/boilerplate.css  - NONE/- text/css \\\"http://m.88yangsheng.com/xieemanhua/\\\" \\\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_2_2 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) BaiduBoxApp/10.2.5 iPhone; CPU iPhone OS 11_2_2 like Mac OS X Mobile/15C202 Safari/602.1 baiduboxapp/10.2.5.11 (Baidu; P2 11.2.2)\\\" \\\"-\\\"\",\"@version\":\"1\",\"@timestamp\":\"2018-01-22T06:45:42.855Z\",\"type\":\"fc_access\",\"file\":\"/data/proclog/log/squid/access.log\",\"host\":\"CNC-LQ-c-3HA\",\"offset\":\"25149271\"}"

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
        println("error parser  CommenParseLog 49")
        println(ex)
        println(kafka_log)
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

    //这里是由于很多时间解析异常不合理 所以在此打印
    if (!timestampStr.contains("201801")){
      throw new ArithmeticException("You are not eligible")
    }

    val clientIp = messageArray(2)

    val repSize = messageArray(4)
    //由于有解析异常 所以这里开始解析  解析成功就ok  解析失败报异常
    Integer.parseInt(repSize)

    val requestUrl = messageArray(6)
    val requestUrlArray = requestUrl.split("/").filterNot(_.isEmpty)
    val uriHost = requestUrlArray(1)

    val mime = messageArray(9)


    val agentCheck = message.split("\"").filterNot(" "==_)(2)

    val userId = WeblukerSqlQueryDict.domainUserDict.getOrElse(uriHost,"")

    val browserOsTup = ClientInfoUtil.getBrowserOs(agentCheck)

    val wlkAddress = IPSeekerUtil.getWlkAddressByIp(clientIp);

    val (countryCN,countryPY,areaCN,areaPY) = (wlkAddress(0),wlkAddress(1),wlkAddress(2),wlkAddress(3))

    //    (timestampStr,clientIp,repSize,uriHost,mime)
    Map(("uriHost",uriHost),("timestampStr",timestampStr),("clientIp",clientIp),("repSize",repSize),("mime",mime),("os_name",browserOsTup._2),("browser_name",browserOsTup._1),("userId",userId),("countryCN",countryCN),("areaCN",areaCN)) //,("countryPY",countryPY),("areaPY",areaPY)
  }

}
