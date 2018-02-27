import java.text.SimpleDateFormat

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf




object KafKaToHadoop_copy1 {

  val jsonParser = new JSONParser()

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = Array("kafka-zk1:2181,kafka-zk2:2181","sp_ka_hdfs","test","5")
    val sparkConf = new SparkConf().setAppName("spark_kafka").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60)) //这个时间设置多长合适？
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val saveRdd = data.map(
      logValue => {
        try {
          val jsonObj: JSONObject = jsonParser.parse(logValue._2).asInstanceOf[JSONObject]
          val message = jsonObj.getAsString("message")
          val host = jsonObj.getAsString("host")
          val messageArray = message.split(" ").filter(!"   ".contains(_)) //这里的过滤影响了效率
          val timestamp = messageArray(0)
          val timestampS: java.lang.String = timestamp.replace(".", "")
          val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
          val timestampStr = dateFormat.format(new java.util.Date(new java.lang.Long(timestampS)))

          val timestampStr_day = timestampStr.substring(0,8)
          val timestampStr_hour = timestampStr.substring(0,10)



          (timestampStr_day,host + "  " + message)
//          host + "  " + message
        }catch {
          case  ex: Exception => {
            ("","")
//            "dsfdsfsdf"
          }
        }
      }
    )
    //.saveAsTextFiles("hdfs://hdfs-master-ip:9000/weblukerOriginLog2/{timestampStr_day}/{timestampStr_hour}",".log")
//    saveRdd.saveAsHadoopFiles("hdfs://hdfs-master-ip:9000/weblukerOriginLog2","sdffdsa", classOf[String],
//      classOf[String],
//      classOf[RDDMultipleTextOutputFormat],classOf[GzipCodec])
//    saveRdd.saveAsHadoopFiles("hdfs://hdfs-master-ip:9000/weblukerOriginLog2/{timestampStr_day}/{timestampStr_hour}",".log",)

//    saveRdd.saveAsHadoopFile("/tmp/lxw1234.com/",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])


    //    saveRdd.foreachRDD(rdd => {
//        rdd.foreach(
//          println(_)
//        )
//
//    })

//    saveRdd.foreachRDD(
//      rdd =>
//        {
//          rdd.saveAsTextFile("hdfs://hdfs-master-ip:9000/weblukerOriginLog2/fdsafds/{timestampStr_day}")
//        }
//    )

    saveRdd.saveAsHadoopFiles(
      "hdfs://hdfs-master-ip:9000/weblukerOriginLog2/mykey/",
      ".log",
      classOf[String],
      classOf[String],
      classOf[RDDMultipleTextOutputFormat[_, _]])




    ssc.start()
    ssc.awaitTermination()
  }

}


class RDDMultipleTextOutputFormat[K, V]() extends MultipleTextOutputFormat[K, V]() {
  override def generateFileNameForKeyValue(key: K, value: V, name: String) : String = {
    (key + "/" + name)
  }
}