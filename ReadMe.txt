博客地址：http://blog.csdn.net/u014171282/article/details/79389333


集群线上运行成功代码
filbeat6.1.1  ->  logstash6.1.1 -> kafka_2.11-1.0.0 -> spark-1.6.1-bin-hadoop2.6 - elasticdearch-5.5.2


第一个流量计算Demo
nohup $SPARK_HOME/bin/spark-submit --class RequestCountTe  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &


第二个进行域名的请求数计算   存入es的格式安装现在的线上环境
nohup $SPARK_HOME/bin/spark-submit --class TotalIPCountUserMySQl  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &


第二个进行域名的请求数计算   存入es的格式安装现在的线上环境  增加异常捕获 请求日志计算成功
nohup $SPARK_HOME/bin/spark-submit --class TotalIPCountDomain  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &


第三个 TotalIPCountUserMySQl
这个类是查询数据库 请求数可以用户查询成功 线上测试
nohup $SPARK_HOME/bin/spark-submit --class TotalIPCountUserMySQl  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &


第四个进行离线计算   OfflineTotalIpDountDomain
从hadoop取出原始日志 然后进行计算存到es中
$SPARK_HOME/bin/spark-submit --class offline.OfflineTotalIpDountDomain  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar 20171204



第四个进行离线计算   OfflineTotalIpDountUserMySQl
从hadoop取出原始日志 然后进行计算存到es中  这里进行了数据库查询 存的是userId

$SPARK_HOME/bin/spark-submit --class offline.OfflineTotalIpDountUserMySQl  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar 20171204


//=================
实时统计完成 主类是 Online.CaclMain
现在实现的实时统计共包括
最终线上使用
nohup $SPARK_HOME/bin/spark-submit --class Online.CaclMain  --master spark://sp26:7077 --executor-memory 40G --total-executor-cores 10 --files /home/ubuntu/sparkJar/qqwry.dat /home/ubuntu/sparkJar2/LogBIgData-1.0-SNAPSHOT.jar &


总请求数统计
文件类型统计   PV访问统计
访问分布统计：地区分布和运行商分布
访问终端统计： 浏览器分布  操作系统分布

未实现流量带宽统计，实现很简单，因为现在没有界面展示验证  其实可以存到数据库中





