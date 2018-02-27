import java.text.SimpleDateFormat

import KafKaToHadoop.jsonParser
import net.minidev.json.JSONObject

object test {


  def main(args: Array[String]): Unit = {
      testParesTime()
  }

  def testParesTime(): Unit ={
    val str="{\"offset\":731295029,\"@version\":\"1\",\"message\":\"1512872095.917 275419 125.117.233.73 TCP_REFRESH_MISS/206 95256953 GET http://dhdown.5211game.com/download/360/dl/zjhl_setup360rjgj_all_1.0.3.19_ef.exe  - DIRECT/180.97.183.203 application/octet-stream \\\"-\\\" \\\"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\\\" \\\"-\\\"\",\"prospector\":{\"type\":\"log\"},\"beat\":{\"name\":\"BGP-SM-4-3gf\",\"hostname\":\"BGP-SM-4-3gf\",\"version\":\"6.1.1\"},\"source\":\"/data/cache1/filbeat_conf/logsdir/CHN-SX-3-3gA.dhdown.5211game.com10.log\",\"host\":\"BGP-SM-4-3gf\",\"@timestamp\":\"2018-01-16T06:00:16.814Z\",\"type\":\"log\",\"tags\":[\"beats_input_codec_plain_applied\"]}"
    val jsonObj: JSONObject = jsonParser.parse(str).asInstanceOf[JSONObject]
    val message = jsonObj.getAsString("message")
    val host = jsonObj.getAsString("host")
    val messageArraySplit = message.split(" ")
    val messageArray = messageArraySplit.filter("" != _) //这里的过滤影响了效率
    val timestamp = messageArray(0)
    val timestampS: java.lang.String = timestamp.replace(".", "")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(timestampS)))

    val timestampStr_day = timestampStr.substring(0,8)
    val timestampStr_hour = timestampStr.substring(0,10)



    (timestampStr_day,host + "  " + message)


  }
}
