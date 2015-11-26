#!/bin/bash

spark_logs="/home/mayuresh/spark-1.5.1/logs"
app_prefix="application_1447540585201_"

for appNo in $(seq -f "%04g" 562 564)
do
	mkdir $app_prefix$appNo
	for node in 39 40 41 42 43 45 46 48 49 28
	do
		scp -r xeno-$node:$spark_logs/$app_prefix$appNo/* $app_prefix$appNo/
	done
done
