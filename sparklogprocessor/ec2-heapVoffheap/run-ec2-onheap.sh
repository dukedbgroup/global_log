#!/bin/bash

prefix="~/ec2-heapVoffheap"
folder="ec2-onheap"
repeat=5

for program in "wordcount.sh" "sortkey.sh";

do
	for i in `seq 1 $repeat`;
	do
		bash "$prefix$folder/$program"
	done
done
