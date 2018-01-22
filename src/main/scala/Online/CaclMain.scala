package Online

import Online.utils.{CommenParseLog, LoggerLevels}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.reflect.ClassTag

object CaclMain {


  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    //设置从kafka中获取数据
//    val Array(zkQuorum, group, topics, numThreads) = Array("101.251.98.137:2181,101.251.98.138:2181,101.251.98.139:2181,101.251.98.140:2181,101.251.98.141:2181","online_test","test","5")
    //线上进行kafka获取数据的地址  线上测试
    val Array(zkQuorum, group, topics, numThreads) = Array("101.251.98.70:2181,101.251.98.71:2181,101.251.98.72:2181,101.251.98.73:2181,101.251.98.74:2181,101.251.98.75:2181,101.251.98.76:2181","online_test","kafka_es","7")
    val sparkConf = new SparkConf().setAppName("ip_totalcount")//.setMaster("local[2]")

    //设置存入es地址
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "101.251.98.137,101.251.98.138,101.251.98.139,101.251.98.140,101.251.98.141")
    sparkConf.set("es.port", "9200")

    //启动sparkStreaming获取数据
    val ssc = new StreamingContext(sparkConf, Seconds(60)) //这个时间设置多长合适？
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val originDataRdd = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)


    val originMapRdd = originDataRdd.map(kafka_log_tup => {
      CommenParseLog.parseLogToTup(kafka_log_tup._2)
    }).filter(_.nonEmpty)

    //进行统计的几个方法
    caclRequest(originMapRdd)
    caclMimeType(originMapRdd)
    caclBrowser(originMapRdd)
    caclOs(originMapRdd)
    caclAreaStat(originMapRdd)


    ssc.start()
    ssc.awaitTermination()
  }



  /**
    * 请求数统计
    * @param originMapRdd
    */
  def caclRequest( originMapRdd:DStream[(Map[String,String])]){

    //请求数统计
    val domainRequestRdd = originMapRdd.map( originMapItem => ((originMapItem.getOrElse("uriHost",""),originMapItem.getOrElse("userId",""),originMapItem.getOrElse("timestampStr","")),1)).reduceByKey(_+_)
    //域名请求数保存
    domainRequestRdd.map( domainRequestItem =>

    {
      val uriHost = domainRequestItem._1._1
      val totalIPCount = domainRequestItem._2
      val media_index= domainRequestItem._1._3.substring(0,8)
      val add_time = domainRequestItem._1._3


      Map(("uriHost",uriHost),("totalIPCount",totalIPCount),("media_index",media_index),("add_time",add_time))
    }
    ).foreachRDD(rdd => {
      if (rdd.count() > 0 ) {
              rdd.saveToEs("spark-portal-{media_index}/logstashIndexDF_ip_totalcount")
//        EsSpark.saveToEs(rdd, "spark-portal-{media_index}/logstashIndexDF_ip_totalcount")
      }
    } )


    //用户请求数统计与保存
    val userRequestRdd = domainRequestRdd.map(domainRequestItem => {
      //( (useId  add_time)  count)
      ((domainRequestItem._1._2,domainRequestItem._1._3),domainRequestItem._2)
    }).reduceByKey(_+_)


    userRequestRdd.map( item =>
      Map("userId" -> item._1._1,"add_time" -> item._1._2,"totalIPCountSum" -> item._2,"media_index" -> item._1._2.substring(0,8)))
      .foreachRDD(
        rdd =>
          {

            if (rdd.count() > 0 ){
              EsSpark.saveToEs(rdd,"spark-portal-{media_index}/logstashIndexDF_ip_totalcount_sum")
            }
          }
//        rdd.saveToEs("spark-portal-{media_index}/logstashIndexDF_ip_totalcount_sum")
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
}
