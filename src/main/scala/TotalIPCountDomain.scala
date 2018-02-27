import org.apache.spark.SparkConf
import org.apache.spark.network.client.TransportClient
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import parlog.TotalIPCount
/**
  * 进行请求数统计 模拟线上代码
  * 首先根据域名来计算  logstashIndexDF_ip_totalcount
  *
  *
  * 
  es数据
{
"_index": "spark-portal-20171121",
"_type": "logstashIndexDF_ip_totalcount",
"_id": "AV_auShQMHmvdwxP_0ou",
"_version": 1,
"_score": 1,
"_source": {
"totalIPCount": 3123,
"uriHost": "img.11static.com",
"userId": 33358,
"add_time": "201711210030"
}
}
  */
object TotalIPCountDomain {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = Array("kafka-zk1:2181,kafka-zk2:2181,kafka-zk3:2181,kafka-zk4:2181,kafka-zk5:2181","g1","test","5")
    val sparkConf = new SparkConf().setAppName("ip_totalcount")//.setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "es1,es2,es3,es4,es5")
    sparkConf.set("es.port", "9200")

    val ssc = new StreamingContext(sparkConf, Seconds(60)) //这个时间设置多长合适？
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val data_mes = data.map( kafka_log_tup => {
//      println("进行解析")
      TotalIPCount .parseLog(kafka_log_tup)
    }).reduceByKey(_+_)

    data_mes.foreachRDD( rdd => {

      if (rdd.count()>0){
        val result = rdd.map((uri_time_tup) => Map("uriHost" -> uri_time_tup._1._1,"add_time" -> uri_time_tup._1._2,"totalIPCount" -> uri_time_tup._2,"media_index" -> uri_time_tup._1._2.substring(0,8)))

        //https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html#spark-streaming-write-dyn
        EsSpark.saveToEs(result,"spark-portal-{media_index}/logstashIndexDF_ip_totalcount")
      }}
    )

    ssc.start()
    ssc.awaitTermination()




  }
}
