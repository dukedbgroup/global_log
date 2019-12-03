#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor"
for folder in "sortkey-sort" "sortkey-tungsten" "sortkey-hash" "sortkey-cons"
do
	for file in "16" "8" "4" "2"
	do
		bash "$prefix/$folder/$file.sh"
	done
done
