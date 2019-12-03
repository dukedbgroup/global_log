#!/bin/bash
echo "Parsing events: $1"

if [[ -z "$1" ]]; then
  exit
fi
first=$1

if [[ -z "$2" ]]; then
  last=$1
else
  last=$2
fi

appprefix="application_1542628163302_"
appsuffix=""

for appNo in $(seq -f "%04g" $first $last)
do
   mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.FlattenTaskMetrics" -Dexec.args="hdfs://xeno-62:9000/sparkEventLog/$appprefix$appNo$appsuffix" -Dexec.cleanupDaemonThreads=false
done
