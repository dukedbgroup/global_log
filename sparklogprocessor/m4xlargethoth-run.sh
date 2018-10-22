#APPNO=$1
DRI='4404m'
MEM=$2
#'4404m'
YARN=$(expr 1024 \* $MEM / 4404)
SUFF='m'
MEM="$MEM$SUFF"
YARN="$YARN$SUFF" 
#'1024m'
FRAC=$5
RATIO=$6
CORES=$3
EXECS=$4
PARALL=$(expr $EXECS \* $CORES)
appprefix="application_1536892175528_"
appsuffix=""

<<C1
# Micro
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.Micro \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Wordcount/Input" 1.2 2 


#pagerank
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.graphx.LiveJournalPageRank \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Pagerank/Input/edges" --numEPart=120 --numIter=2 --storageLevel="MEMORY_ONLY"


# K-means
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
--jars /home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 3 --storageLevel "MEMORY_ONLY"


#sort
hadoop dfs -rmr /output
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.Sort \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.yarn.am.cores=$CORES --conf spark.yarn.am.memory=$MEM \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Sort/Input-512/Input" "hdfs://xeno-62:9000/output"

<<C
#wordcount
hadoop dfs -rmr /output
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.WordCount \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Sort/Input" "hdfs://xeno-62:9000/output"
C1

#SVM
hadoop dfs -rmr /SparkBench/SVM/Output
~/spark-2.0.1/bin/spark-submit \
--class SVM.src.main.java.SVMApp \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-bench/SVM/target/SVMApp-1.0.jar /SparkBench/SVM/Input /SparkBench/SVM/Output 2 MEMORY_ONLY

<<C
#TPCH-9
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.tpch.TPCH \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory $DRI --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.default.parallelism=$PARALL \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf spark.driver.maxResultSize=2g \
--conf spark.dynamicAllocation.enabled=false \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/tpch50" 9 
C
first=$1
last=$1
#$(expr $first + 1)
for appNo in $(seq -f "%04g" $first $last)
do
# copy logs
cd ~/heap-logs; ~/heap-logs/copyLogs.sh $appNo

#events to db
cd ~/global_log/sparklogprocessor; ./spark-events-to-db.sh $appNo

#summarize stas
# ./summarize-stats.sh $appNo
done
