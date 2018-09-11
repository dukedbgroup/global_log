appprefix="application_1535007510432_"
appsuffix=""

if [[ -z "$1" ]]; then
  exit
fi
APPNO=$1

MEM='2G'
if [[ "$2" ]]; then
  MEM=$2
fi

FRAC=0.6
if [[ "$3" ]]; then
  FRAC=$3
fi

RATIO=2
if [[ "$4" ]]; then
  RATIO=$4
fi

for N in $(seq -f "%g" 1 22)
do

~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.tpch.TPCH \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory $MEM --executor-cores 1 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=1G \
--conf spark.memory.fraction=$FRAC \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/tpch50" $N

# copy logs
cd ~/heap-logs; ~/heap-logs/copyLogs.sh $APPNO

#events to db
cd ~/global_log/sparklogprocessor; ./spark-events-to-db.sh $APPNO

#summarize stas
./summarize-stats.sh $APPNO

((APPNO++))

done
