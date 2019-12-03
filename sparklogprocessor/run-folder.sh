#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor/"
folder="shuffle-fraction"
repeat=2

for program in "wordcount.sh";

do
	for i in `seq 1 $repeat`;
	do
		bash "$prefix$folder/$program"
	done
done
