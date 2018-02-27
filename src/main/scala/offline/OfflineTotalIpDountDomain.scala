package offline
import parlog.TotalIPCount
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object OfflineTotalIpDountDomain {

  def main(args: Array[String]): Unit = {

        val caclDate = args(0)
//    val caclDate = "20171204"

//    val AppName = "OfflineTotalIpDount"
    val AppName = "OfflineTotalIpDount" + caclDate

    //    val HdInputFilePath = args(0)
//    val HdOutputFilePath = args(1)

//    val HdInputFilePath = "hdfs://hdfs-master-ip:9000/weblukerOriginLog/20171204"
    val HdInputFilePath = "hdfs://hdfs-master-ip:9000/weblukerOriginLog/"+ caclDate
//    val HdOutputFilePath = args(1)

    val conf = new SparkConf().setAppName(AppName)//.setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "es-ip1,es-ip2,es-ip3,es-ip4,es-ip5")
    conf.set("es.port", "9200")
    val sc = new SparkContext(conf)

    val result_rdd = sc.textFile(HdInputFilePath).map(
      hdfsOriginLog => {
        ParseHdfsOriginLog.parseHdfsOriginLog(hdfsOriginLog)
      }
    ).reduceByKey(_+_)
      .map((uri_time_tup) => Map("uriHost" -> uri_time_tup._1._1,"add_time" -> uri_time_tup._1._2,"totalIPCount" -> uri_time_tup._2,"media_index" -> uri_time_tup._1._2.substring(0,8)))

    EsSpark.saveToEs(result_rdd,"spark-portal-{media_index}/logstashIndexDF_ip_totalcount")

    sc.stop()

  }







}
