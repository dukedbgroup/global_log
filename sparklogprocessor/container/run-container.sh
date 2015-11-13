#!/bin/bash

prefix="/home/mayuresh/global_log/sparklogprocessor/"
folder="container"
program="pagerank.sh"
repeat=5

for i in `seq 1 $repeat`;
do
	bash "$prefix$folder/$program"
done
