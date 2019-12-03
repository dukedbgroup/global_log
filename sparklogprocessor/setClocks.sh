#!/bin/bash

#spark_logs="/home/mayuresh/spark-logs"
#app_prefix="application_1481331662402_"

#for appNo in $(seq -f "%04g" 24 24)
#do
#	mkdir $app_prefix$appNo
	for node in 71 40 41 42 43 45 46 48 49 44
	do
		ssh -t xeno-$node 'sudo timedatectl set-time "2017-01-10 09:38:55"'
	done
#done
