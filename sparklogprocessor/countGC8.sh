#!/bin/bash

#appId="application_1447540585201_$1" 
appId="application_1447540585201_$1"
prefix="sparkOutput_worker_"
suffix=".txt"

#for appNo in $(seq -f "%04g" 462 462)
#do
	old=0
	young=0
	oldGC=0
	youngGC=0
	maxYoung=0
	totalYoung=0
	maxOld=0
	totalOld=0
	diff=0

#execId = "6"
#for execId in "1" "2" "3" "4" "5" "6" "7" "8" "9" "10"
for execId in `find ~/heap-logs/$appId/ -mindepth 1 -maxdepth 1 -type d`
do
        #file=~/heap-logs/$appId/$execId/$prefix$appId\_$execId$suffix
        file=`ls $execId/$prefix*`
	echo "reading "$file

	while read -r one two three four five six seven 
	do
		#echo "$two"
		if (($three+$four < $young)); then
			((diff=young-three-four))
			((diff=diff/1024/1024))
			((totalYoung=totalYoung+diff))
			if (($diff > $maxYoung)); then
				((maxYoung=diff))
			fi
			((youngGC=youngGC+1))
		fi
		if (($five < $old)); then
			((diff=old-five))
			((diff=diff/1024/1024))
			((totalOld=totalOld+diff))
			if (($diff > $maxOld)); then
				((maxOld=diff))
			fi
			((oldGC=oldGC+1))
		fi
		((young=three+four))
		((old=five))
	done < "$file"
done
	echo "# young GC : $youngGC, #old GC: $oldGC"
	((totalYoung=totalYoung/youngGC))
	((totalOld=totalOld/oldGC))
	echo "avg young GC : $totalYoung, #avg old GC : $totalOld"
	echo "max young GC : $maxYoung, max old GC : $maxOld"
#done
