#!/bin/bash

appprefix="application_1447540585201_"
appsuffix=""

for appNo in $(seq -f "%04g" 1400 1400)
do
   mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.FlattenTaskMetrics" -Dexec.args="hdfs://xeno-62:9000/sparkEventLog/$appprefix$appNo$appsuffix"
done
