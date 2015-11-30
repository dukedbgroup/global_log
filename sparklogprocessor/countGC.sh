#!/bin/bash

appId="application_1447540585201_0592"
execId="6"
prefix="sparkOutput_worker_"
suffix=".txt"

#for appNo in $(seq -f "%04g" 462 462)
#do
	file=~/heap-logs/$appId/$execId/$prefix$appId\_$execId$suffix
	echo "reading "$file
	old=0
	young=0
	oldGC=0
	youngGC=0
	maxYoung=0
	totalYoung=0
	maxOld=0
	totalOld=0
	diff=0
	while read -r one two three four five six seven 
	do
		#echo "$two"
		if (($two+$three < $young)); then
			((diff=young-two+three))
			((diff=diff/1024/1024))
			((totalYoung=totalYoung+diff))
			if (($diff > $maxYoung)); then
				((maxYoung=diff))
			fi
			((youngGC=youngGC+1))
		fi
		if (($four < $old)); then
			((diff=old-four))
			((diff=diff/1024/1024))
			((totalOld=totalOld+diff))
			if (($diff > $maxOld)); then
				((maxOld=diff))
			fi
			((oldGC=oldGC+1))
		fi
		((young=two+three))
		((old=four))
	done < "$file"
	echo "# young GC : $youngGC, #old GC: $oldGC"
	((totalYoung=totalYoung/youngGC))
	((totalOld=totalOld/oldGC))
	echo "avg young GC : $totalYoung, #avg old GC : $totalOld"
	echo "max young GC : $maxYoung, max old GC : $maxOld"
#done
