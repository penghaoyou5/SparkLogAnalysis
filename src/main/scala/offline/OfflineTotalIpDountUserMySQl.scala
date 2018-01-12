package offline

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import utils.WeblukerSqlQueryDict

object OfflineTotalIpDountUserMySQl {

  def main(args: Array[String]): Unit = {

    //20171204
    val caclDate = args(0)
//    val caclDate = "20171205"

//    val AppName = "OfflineTotalIpDount"
    val AppName = "OfflineTotalIpDount" + caclDate
//    val HdInputFilePath = args(0)
//    val HdOutputFilePath = args(1)

//    val HdInputFilePath = "hdfs://101.251.98.137:9000/weblukerOriginLog/20171204"

    val HdInputFilePath = "hdfs://101.251.98.137:9000/weblukerOriginLog/"+ caclDate
//    val HdOutputFilePath = args(1)

    val conf = new SparkConf().setAppName(AppName)//.setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "101.251.98.137,101.251.98.138,101.251.98.139,101.251.98.140,101.251.98.141")
    conf.set("es.port", "9200")
    val sc = new SparkContext(conf)

    val result_rdd = sc.textFile(HdInputFilePath).map(
      hdfsOriginLog => {
        ParseHdfsOriginLog.parseHdfsOriginLog(hdfsOriginLog)
      }
    ).reduceByKey(_+_)



    val user_result_rdd = result_rdd.map(log_domain_tup =>  (( WeblukerSqlQueryDict.domainUserDict.getOrElse(log_domain_tup._1._1,0),log_domain_tup._1._2),log_domain_tup._2) )
      .filter(_._1._1 != 0).reduceByKey(_+_)

    val es_result = user_result_rdd.map((uri_time_tup) =>Map("userId" -> uri_time_tup._1._1,"add_time" -> uri_time_tup._1._2,"totalIPCountSum" -> uri_time_tup._2,"media_index" -> uri_time_tup._1._2.substring(0,8)))

//    EsSpark.saveToEs(es_result,"spark-portal-{media_index}/logstashIndexDF_ip_totalcount")

    EsSpark.saveToEs(es_result,"spark-portal-{media_index}/logstashIndexDF_ip_totalcount_sum")

    sc.stop()

  }


}
