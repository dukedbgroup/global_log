#!/bin/bash
echo "summarizing for: $1"

if [[ -z "$1" ]]; then
  exit
fi
first=$1

if [[ -z "$2" ]]; then
  last=$1
else
  last=$2
fi

appprefix="application_1535007510432_"
appsuffix=""

appName="micro"
maxHeap=$((2*1024*1024*1024))
yarnOverhead=$((1*1024*1024*1024))
#following is for Micro
stageDAG="2->0#3->2#5->0#7->0#8->7#10->0#12->0#13->12"
#following is for KMeans
#stageDAG="1->0#2->0,1#3->0,1,3#4->0,1,3#5->0,1,5#6->0,1,5#7->0,1,7#8->0,1,7#9->0,1,9#10->0,1,9#11->0,1,11#12->0,1,11#13->0,1#14->13#15->0,1#16->15#17->0,1#18->17#19->0,1#20->19"
totalStages=22

for appNo in $(seq -f "%04g" $first $last)
do
           ARGS="$appprefix$(printf %04g $appNo)$appsuffix $appName $maxHeap $yarnOverhead $stageDAG $totalStages"
           mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.TrainExecutors" -Dexec.args="$ARGS" -Dexec.cleanupDaemonThreads=false
done

