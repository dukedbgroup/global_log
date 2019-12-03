appprefix="application_1542628163302_"
appsuffix=""

if [[ -z "$1" ]]; then
  exit
fi
APPNO=$1

MEM=4404
if [[ "$2" ]]; then
  MEM=$2
fi

YARN=$(expr 1024 \* $MEM / 4404)
SUFF='m'
MEM=$MEM$SUFF
YARN=$YARN$SUFF

CORES=2
if [[ "$3" ]]; then
  CORES=$3
fi

EXECS=8
if [[ "$4" ]]; then
  EXECS=$4
fi

FRAC=0.6
if [[ "$5" ]]; then
  FRAC=$5
fi

RATIO=2
if [[ "$6" ]]; then
  RATIO=$6
fi

N=2
if [[ "$7" ]]; then
  N=$7
fi

#for N in $(seq -f "%g" 1 22)
#do

~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.tpch.TPCH \
--master yarn --deploy-mode client \
--num-executors $EXECS \
--driver-memory 1G --executor-memory $MEM --executor-cores $CORES \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=$YARN \
--conf spark.memory.fraction=$FRAC \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=$RATIO -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/tpch50" $N

# copy logs
cd ~/heap-logs; ~/heap-logs/copyLogs.sh $APPNO

#events to db
cd ~/global_log/sparklogprocessor; ./spark-events-to-db.sh $APPNO

#summarize stas
#./summarize-stats.sh $APPNO

((APPNO++))

#done
