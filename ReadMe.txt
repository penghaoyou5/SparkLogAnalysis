集群线上运行成功代码
filbeat6.1.1  ->  logstash6.1.1 -> kafka_2.11-1.0.0 -> spark-1.6.1-bin-hadoop2.6 - elasticdearch-5.5.2


第一个流量计算Demo
nohup $SPARK_HOME/bin/spark-submit --class RequestCountTe  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &


第二个进行域名的请求数计算   存入es的格式安装现在的线上环境
nohup $SPARK_HOME/bin/spark-submit --class TotalIPCountUserMySQl  --master spark://sp26:7077 --executor-memory 20G --total-executor-cores 10 /home/ubuntu/sparkJar/LogBIgData-1.0-SNAPSHOT.jar &
