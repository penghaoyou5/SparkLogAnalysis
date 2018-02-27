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

/**
  * 解析日志格式
  * {"message":"1514350489.468      1 222.218.57.196 TCP_MEM_HIT/200 352 GET http://laest.kh92.cn/new_old/new_sf.zip.crc?id=967  - NONE/- application/x-msdownload \"-\" \"-\" \"-\"","@version":"1","@timestamp":"2017-12-27T04:54:50.966Z","type":"fc_access","file":"/data/proclog/log/squid/access.log","host":"CHN-WX-b-3H9","offset":"2598752"}
  *
  */
object RequestCountTe {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()



    val Array(zkQuorum, group, topics, numThreads) = Array("kafka-zk1:2181,kafka-zk2:2181,kafka-zk3:2181,kafka-zk4:2181,kafka-zk5:2181","g1","test","2")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")//.setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "es-ip1,es-ip2,es-ip3,es-ip4,es-ip5")
    sparkConf.set("es.port", "9200")

    val ssc = new StreamingContext(sparkConf, Seconds(60)) //这个时间设置多长合适？
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val data_mes = data.map( kafka_log_tup => {
      println("进行解析")
        ParseLog.parseLogForBwCatch(kafka_log_tup)
    }).filter( value => {value._2 != 0} ).reduceByKey(_+_).mapValues(_/60)

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

