#!/bin/bash
echo "Plotting for: $1"

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

for appNo in $(seq -f "%04g" $first $last)
do
   mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.HeapUsageCPUPlotter" -Dexec.args="$appprefix$appNo$appsuffix"
done
