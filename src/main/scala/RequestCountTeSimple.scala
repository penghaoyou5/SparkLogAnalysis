import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object RequestCountTeSimple {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()



    val Array(zkQuorum, group, topics, numThreads) = Array("101.251.98.137:2181","g1","test","2")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")//.setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
//    sparkConf.set("es.nodes", "101.251.98.144")
    sparkConf.set("es.nodes", "101.251.98.139")
    sparkConf.set("es.port", "9200")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    data.foreachRDD( rdd => {
      if (rdd.count()>0){
        val result = rdd.map((uri_time_tup) => Map("uriHost" -> "uriHostfasd","timestamp" -> "54345","reqSize" -> "23432"))


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

