#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor"
for folder in "sortkey-sort" "sortkey-hash" "sortkey-tungsten" "wordcount-sort" "wordcount-hash"
do
	for file in "32" "16" "8" "4" "2"
	do
		bash "$prefix/$folder/$file.sh"
	done
done
