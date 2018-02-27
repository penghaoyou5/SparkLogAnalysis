package Online

import Online.utils.{CommenParseLog, KafkaSink, LoggerLevels, MySparkKafkaProducer}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}
import org.apache.spark.broadcast.Broadcast
import java.util.{Calendar, Date, Locale, Properties}

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.concurrent.Future

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object CaclMain {


  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    //设置从kafka中获取数据
//    val Array(zkQuorum, group, topics, numThreads) = Array("kafka-zkip1:2181,kafka-zkip2:2181,kafka-zkip3:2181,kafka-zkip4:2181,kafka-zkip5:2181","online_test","test","5")
    //线上进行kafka获取数据的地址  线上测试
    val Array(zkQuorum, group, topics, numThreads) = Array("kafka-zkip1:2181,kafka-zkip2:2181,kafka-zkip3:2181,kafka-zkip4:2181,kafka-zkip5:2181,kafka-zkip6:2181","online_test","kafka_es","5")
    val sparkConf = new SparkConf().setAppName("ip_totalcount_mapout")//.setMaster("local[2]")

    //设置存入es地址
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "es-ip1,es-ip2,es-ip3,es-ip4,es-ip5")
    sparkConf.set("es.port", "9200")

    //启动sparkStreaming获取数据
    val ssc = new StreamingContext(sparkConf, Seconds(60)) //这个时间设置多长合适？
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val originDataRdd = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)


//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ss)
//    val originDataRdd = originDataRddNoCache.cache()


//    val kafkaProducer: Broadcast[MySparkKafkaProducer[Array[Byte], String]] = {
//      val kafkaProducerConfig = {
//        val p = new Properties()
//        p.setProperty("bootstrap.servers", "kafka-zk1:9092,kafka-zk2:9092")
//        p.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
//        p.setProperty("value.serializer", classOf[StringSerializer].getName)
//        p
//      }
//      ssc.sparkContext.broadcast(MySparkKafkaProducer[Array[Byte], String](kafkaProducerConfig))
//    }


    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "kafka-zk1:9092,kafka-zk2:9092,kafka-zk3:9092,kafka-zk4:9092,kafka-zk5:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
//      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    val originMapRdd = originDataRdd.map(kafka_log_tup => {
      CommenParseLog.parseLogToTup(kafka_log_tup._2)
    }).filter(_.nonEmpty)

    //进行统计的几个方法
    caclRequest(originMapRdd,kafkaProducer,ssc)
//    caclMimeType(originMapRdd)
//    caclBrowser(originMapRdd)
//    caclOs(originMapRdd)
//    caclAreaStat(originMapRdd)


    ssc.start()
    ssc.awaitTermination()
  }





  /**
    * 请求数统计
    * @param originMapRdd
    */
  def caclRequest( originMapRdd:DStream[(Map[String,String])],kafkaProducer:Broadcast[KafkaSink[String, String]],ssc: StreamingContext){

    //请求数统计
//    val domainRequestRdd = originMapRdd.map( originMapItem => ((originMapItem.getOrElse("uriHost",""),originMapItem.getOrElse("userId",""),originMapItem.getOrElse("timestampStr","")),1)).reduceByKey(_+_)

    val domainRequestRdd = originMapRdd.map( originMapItem => {

      print("qing qiu map originMapRdd.map" + originMapItem.getOrElse("timestampStr",""))
      ((originMapItem.getOrElse("uriHost",""),originMapItem.getOrElse("userId",""),originMapItem.getOrElse("timestampStr","")),1)}).reduceByKey((i1,i2) => {

      print("qing qiu map originMapRdd.reduce " + i2)
      i1+i2})


    //域名请求数保存
    domainRequestRdd.map( domainRequestItem =>

    {
      val uriHost = domainRequestItem._1._1
      val totalIPCount = domainRequestItem._2
      val media_index= domainRequestItem._1._3.substring(0,8)
      val add_time = domainRequestItem._1._3
      println( "shuchu  foreachRDD domainRequestItem    kanak ")
      kafkaProducer.value.send("my-output-topic", add_time + "domainRequestRdd.map")


      Map(("uriHost",uriHost),("totalIPCount",totalIPCount),("media_index",media_index),("add_time",add_time))
    }
    ).foreachRDD(rdd =>
        saveRddToKafka(rdd,kafkaProducer,ssc)
    )


    //用户请求数统计与保存
    val userRequestRdd = domainRequestRdd.map(domainRequestItem => {
      //( (useId  add_time)  count)
      ((domainRequestItem._1._2,domainRequestItem._1._3),domainRequestItem._2)
    }).reduceByKey(_+_)


    userRequestRdd.map( item =>
      Map("userId" -> item._1._1,"add_time" -> item._1._2,"totalIPCountSum" -> item._2,"media_index" -> item._1._2.substring(0,8)))
      .foreachRDD(rdd =>
        saveRddToKafka(rdd,kafkaProducer,ssc)
      )
  }







  /**
    * 进行请求类型mime统计
    * @param originMapRdd
    */
  def caclMimeType(originMapRdd:DStream[(Map[String,String])]){
     val domainMimeRdd = originMapRdd.map(item  => {((item.getOrElse("uriHost",""),item.getOrElse("userId",""),item.getOrElse("mime",""),item.getOrElse("timestampStr","")),(1,Integer.parseInt(item.getOrElse("repSize","0"))))} ).reduceByKey((item1,item2) => (item1._1+item2._1,item1._2+item2._2))

    //按照域名统计保存
    domainMimeRdd.map(item => Map(("uriHost",item._1._1),("mime",item._1._3),( "fileCount",item._2._1),("totalFileSize",item._2._2),("media_index",item._1._4.substring(0,8)),("add_time",item._1._4))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_filetype_totalsize"))



    //请求类型按照用户保存
    val useMimeRdd = domainMimeRdd.map(item => ((item._1._2,item._1._3,item._1._4),item._2)).reduceByKey((item1,item2) => (item1._1+item2._1,item1._2+item2._2))

    useMimeRdd.map(item => Map(("userId",item._1._1),("mime",item._1._2),("fileCountSum",item._2._1),("totalFileSizeSum",item._2._2),("media_index",item._1._3.substring(0,8)),("add_time",item._1._3))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_filetype_totalsize_sum"))



    //==========  顺便进行pageView统计

    //pv按域名统计 保存
    domainMimeRdd.filter(_._1._3 == "text/html").map(item => Map(("uriHost",item._1._1), ( "fileCount",item._2._1),("media_index",item._1._4.substring(0,8)),("add_time",item._1._4))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_pv_count"))

    //pv按用户统计 保存
    useMimeRdd.filter(_._1._3 == "text/html").map(item => Map(("userId",item._1._1),("pvCountSum",item._2._1),("media_index",item._1._3.substring(0,8)),("add_time",item._1._3) )).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_pv_count_sum"))

  }


  /**
    * 进行浏览器统计
    * @param originMapRdd
    */
  def caclBrowser(originMapRdd:DStream[(Map[String,String])]): Unit ={
    //=======进行浏览器统计
    //域名统计
    val domainBrowserRdd = originMapRdd.map(item => ((item.getOrElse("uriHost",""),item.getOrElse("userId",""),item.getOrElse("browser_name",""),item.getOrElse("timestampStr","")),1)).reduceByKey(_+_)

    //统计按域名保存
    domainBrowserRdd.map(item => Map(("uriHost",item._1._1),("name",item._1._3),("browserCount",item._2),("media_index",item._1._4.substring(0,8)),("add_time",item._1._4))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_browser_count"))


    //在域名基础上用户统计  保存
    val useBrowserRdd = domainBrowserRdd.map(item => ((item._1._2,item._1._3,item._1._4),item._2)).reduceByKey(_+_)

    useBrowserRdd.map(item =>  Map(("userId",item._1._1),("name",item._1._2),("browserCountSum",item._2),("media_index",item._1._3.substring(0,8)),("add_time",item._1._3))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_browser_count_sum"))


  }



  /**
    * 进行系统统计  代码和上面浏览器代码差不多 ，这种代码应该出入固定的参数就行了吧
    * @param originMapRdd
    */
  def caclOs(originMapRdd:DStream[(Map[String,String])]): Unit ={
    //=======进行浏览器统计
    //域名统计
    val domainOsRdd = originMapRdd.map(item => ((item.getOrElse("uriHost",""),item.getOrElse("userId",""),item.getOrElse("os_name",""),item.getOrElse("timestampStr","")),1)).reduceByKey(_+_)

    //统计按域名保存
    domainOsRdd.map(item => Map(("uriHost",item._1._1),("userId",item._1._2),("os_name",item._1._3),("osCount",item._2),("media_index",item._1._4.substring(0,8)),("add_time",item._1._4))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_os_count"))


//    //在域名基础上用户统计  保存
//    val useOsRdd = domainOsRdd.map(item => ((item._1._2,item._1._3,item._1._4),item._2)).reduceByKey(_+_)
//
//    useOsRdd.map(item =>  Map(("userId",item._1._1),("name",item._1._2),("browserCountSum",item._2),("media_index",item._1._3.substring(0,8)),("add_time",item._1._3))).foreachRDD(_.saveToEs("spark-portal-{media_index}/logstashIndexDF_browser_count_sum"))

  }


  /**
    * 进行区域统计
    * @param originMapRdd
    */
  def caclAreaStat(originMapRdd:DStream[(Map[String,String])]): Unit ={
    //域名统计
    val domainAreaStatRdd = originMapRdd.map(item => ((item.getOrElse("uriHost",""),item.getOrElse("userId",""),item.getOrElse("countryCN",""),item.getOrElse("areaCN",""),item.getOrElse("timestampStr","")),1)).reduceByKey(_+_)

    //统计按域名保存
    domainAreaStatRdd.map(item => Map(("uriHost",item._1._1),("countryCN",item._1._3),("areaCN",item._1._4),("mimeIPCount",item._2),("media_index",item._1._5.substring(0,8)),("add_time",item._1._5))).foreachRDD(_.saveToEs("spark-portal-area-{media_index}/logstashIndexDF_area_count"))


    //在域名基础上用户统计  保存
    val useBrowserRdd = domainAreaStatRdd.map(item => ((item._1._2,item._1._3,item._1._4,item._1._5),item._2)).reduceByKey(_+_)

    useBrowserRdd.map(item =>  Map(("userId",item._1._1),("countryCN",item._1._2),("areaCN",item._1._3),("mimeIPCountSum",item._2),("media_index",item._1._4.substring(0,8)),("add_time",item._1._4))).foreachRDD(_.saveToEs("spark-portal-area-{media_index}/logstashIndexDF_area_count_sum"))

  }



  def saveRddToKafka(rdd:RDD[Map[String,Any]],kafkaProducer_old:Broadcast[KafkaSink[String, String]],ssc: StreamingContext): Unit ={
    println( "shuchu  foreachRDD saveRddToKafka ")

    if (!rdd.isEmpty) {

//      // 广播KafkaSink
//      val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
//        val kafkaProducerConfig = {
//          val p = new Properties()
//          p.setProperty("bootstrap.servers", "kafka-zk1:9092,kafka-zk2:9092")
//          p.setProperty("key.serializer", classOf[StringSerializer].getName)
//          p.setProperty("value.serializer", classOf[StringSerializer].getName)
//          p
//        }
//        //      log.warn("kafka producer init done!")
//        ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
//      }


      kafkaProducer_old.value.send("my-output-topic", "shuchu rddin ")
      println( "shuchu rddin ")

      val strRdd = rdd.map(item => {
//        item.toString()
        item.toBuffer.toString()
      })
      strRdd.foreach(record => {
        kafkaProducer_old.value.send("my-output-topic", record)
        println(record)
        // do something else
      })
    }
  }

}
