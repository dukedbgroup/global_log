#!/bin/bash

if [[ -z "$1" ]]; then
  exit
fi
first=$1

if [[ -z "$2" ]]; then
  last=$1
else
  last=$2
fi

for appNo in $(seq -f "%04g" $first $last)
do
  scala ApplicationStatistic $appNo
done 
