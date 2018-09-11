APPNO=$1
MEM=$2
FRAC=$3
RATIO=$4
appprefix="application_1535007510432_"
appsuffix=""

# run Spark application


~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.Micro \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory $2 --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=1G \
--conf spark.memory.fraction=$3 \
--conf spark.driver.maxResultSize=2g \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$4 -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Wordcount/Input" 1.2 2 

<<C1
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory $2 --executor-cores 1 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=1G \
--conf spark.memory.fraction=$3 \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$4 -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
--jars /home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 3 --storageLevel "MEMORY_ONLY"
C1

<<C
hadoop dfs -rmr /output
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.SortDF \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory $2 --executor-cores 1 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=1G \
--conf spark.memory.fraction=$3 \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$4 -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Sort/Input" "hdfs://xeno-62:9000/output"
C

# copy logs
cd ~/heap-logs; ~/heap-logs/copyLogs.sh $APPNO

#events to db
cd ~/global_log/sparklogprocessor; ./spark-events-to-db.sh $APPNO

#summarize stas
./summarize-stats.sh $APPNO

#pictures
#./create-graph.sh $APPNO
