#!/bin/bash

appprefix="application_1446415553019_"
appsuffix="_1"

for appNo in $(seq -f "%04g" 9 68)
do
   mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.FlattenTaskMetrics" -Dexec.args="hdfs://xeno-62:9000/sparkEventLog/$appprefix$appNo$appsuffix"
done
