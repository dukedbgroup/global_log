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

appprefix="application_1517445963025_"
appsuffix=""

appNo=$first
appName="micro"
memStorage=$((19*1024*1024*1024)) 
maxHeap=$((1*1024*1024*1024))
maxCores=4
yarnOverhead=$((1*1024*1024*1024))
numExecs=10
sparkFraction=0.2
offHeap=0
offHeapSize=0
serializer="java"
GCAlgo="parallel"
newRatio=2
#following is for Micro
#stageDAG="2->0#3->2#5->0#7->0#8->7#10->0#12->0#13->12"
#following is for KMeans
#stageDAG="1->0#2->0,1#3->0,1,3#4->0,1,3#5->0,1,5#6->0,1,5#7->0,1,7#8->0,1,7#9->0,1,9#10->0,1,9#11->0,1,11#12->0,1,11#13->0,1#14->13#15->0,1#16->15#17->0,1#18->17#19->0,1#20->19"
stageDAG="3->2"

for FRAC in 0.2 0.4 0.6 0.8
do
        for RATIO in 2 5 8 11
        do
           ARGS="$appprefix$(printf %04g $appNo)$appsuffix $appName $memStorage $maxHeap $maxCores $yarnOverhead $numExecs $FRAC $offHeap $offHeapSize $serializer $GCAlgo $RATIO $stageDAG"
           mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.TrainRelM" -Dexec.args="$ARGS" -Dexec.cleanupDaemonThreads=false
           ((appNo++))
        done
done

