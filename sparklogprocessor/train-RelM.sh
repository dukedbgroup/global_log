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

appprefix="application_1511969392672_"
appsuffix=""

appNo=$first
appName="micro"
memStorage=$((34*1024*1024*1024))
maxHeap=$((4*1024*1024*1024))
maxCores=2
yarnOverhead=$((1*1024*1024*1024))
numExecs=10
sparkFraction=0.2
offHeap=0
offHeapSize=0
serializer="java"
GCAlgo="parallel"
newRatio=2
cacheStage=5

for FRAC in 0.2 0.4 0.6 0.8
do
        for RATIO in 2 5 8 11
        do
           ARGS="$appprefix$(printf %04g $appNo)$appsuffix $appName $memStorage $maxHeap $maxCores $yarnOverhead $numExecs $FRAC $offHeap $offHeapSize $serializer $GCAlgo $RATIO $cacheStage"
           mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.RelMTraining" -Dexec.args="$ARGS"
           ((appNo++))
        done
done

