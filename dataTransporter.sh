#!/bin/bash

appIDs=(
"application_1448400088838_0327"
#"application_xxx"
#"application_yyy"
)
for appID in "${appIDs[@]}"
do
        echo "transfer $appID:"
        for a in {1..10}
        do
                scp -r yuzhang@slave$a:/home/yuzhang/spark-1.5.1/logs/$appID /home/yuzhang/spark-1.5.1/logs/
                mkdir /home/yuzhang/spark-1.5.1/logs/$appID/slave$a
                scp -r yuzhang@slave$a:/home/yuzhang/hadoop-2.6.0/logs/userlogs/$appID/* /home/yuzhang/spark-1.5.1/logs/$appID/slave$a/
        done
        scp /tmp/spark-events/$appID /home/yuzhang/spark-1.5.1/logs/$appID/
done
