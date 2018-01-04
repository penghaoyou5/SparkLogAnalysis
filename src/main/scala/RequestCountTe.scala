import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._

object RequestCountTe {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()



    val Array(zkQuorum, group, topics, numThreads) = Array("101.251.98.137:2181","g1","test","2")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "101.251.98.137")
    sparkConf.set("es.port", "9200")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val data_mes = data.map( kafka_log_tup => {
      val jsonParser = new JSONParser()
      val kafka_log = kafka_log_tup._2
      val jsonObj: JSONObject = jsonParser.parse(kafka_log).asInstanceOf[JSONObject]
      val message = jsonObj.getAsString("message")
//      message
      val messageArray = message.split(" ").filter(!"   ".contains(_))
      println(messageArray.toBuffer)
      val timestamp = messageArray(0)

      val timestampS:java.lang.String = "1514350489.468".replace(".","")
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
//      DateTimeFormat.forPattern("").parseMillis(timestamp)

      val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(timestampS)))
      println(timestampStr)

      val requestUrl = messageArray(6)
      val requestUrlArray = requestUrl.split("/").filter(!"   ".contains(_))
      println(requestUrlArray.toBuffer)
      val uriHost = requestUrlArray(1)
      val reqSize = messageArray(4).toDouble

      ((uriHost,timestampStr),reqSize)
    }).reduceByKey(_+_).mapValues(_/60)

//    data_mes.print()  (uri_time_tup,bandwidth)
    data_mes.foreachRDD( rdd => {

      if (rdd.count()>0){
       val result = rdd.map((uri_time_tup) => Map("uriHost" -> uri_time_tup._1._1,"timestamp" -> uri_time_tup._1._2,"reqSize" -> uri_time_tup._2))


      EsSpark.saveToEs(result,"logbigdata/bandwidth")
//        println(result.toString())
//        println("resuler 1========")
//        rdd.saveToEs("logbigdata/bandwidth")
    }}
    )

    ssc.start()
    ssc.awaitTermination()
  }

}

