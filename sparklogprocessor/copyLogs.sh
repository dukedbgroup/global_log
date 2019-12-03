#!/bin/bash

spark_logs="/home/mayuresh/spark-logs"
app_prefix="application_1481331662402_"

for appNo in $(seq -f "%04g" 24 24)
do
	mkdir $app_prefix$appNo
	for node in 39 40 41 42 43 45 46 48 49 44
	do
		scp -r xeno-$node:$spark_logs/$app_prefix$appNo/* $app_prefix$appNo/
	done
done
