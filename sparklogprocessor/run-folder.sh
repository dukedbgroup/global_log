#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor/"
folder="container"
repeat=5

for program in "kmeans.sh";
do
	for i in `seq 1 $repeat`;
	do
		bash "$prefix$folder/$program"
	done
done
