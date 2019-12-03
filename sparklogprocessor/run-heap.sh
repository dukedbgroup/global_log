#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor"
for folder in "heap-map"
do
	for file in "16-0.2" "16-0.6"
	do
		bash "$prefix/$folder/$file.sh"
	done
done
