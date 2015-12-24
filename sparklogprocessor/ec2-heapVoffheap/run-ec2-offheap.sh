#!/bin/bash

prefix="~/ec2-heapVoffheap"
folder="ec2-offheap"
repeat=5

for program in "wordcount-offheap.sh" "sortkey-offheap.sh";

do
	for i in `seq 1 $repeat`;
	do
		bash "$prefix$folder/$program"
	done
done
