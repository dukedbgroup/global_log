#!/bin/bash

# sort shuffle 32 cores, shuffle fraction 0.6
hadoop dfs -rmr /output
~/spark-1.5.1/bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 16 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf spark.shuffle.service.enabled=false --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.shuffle.memoryFraction=0.6 --conf spark.storage.memoryFraction=0.2 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.sql.tungsten.enabled=true --conf spark.driver.maxResultSize=1536 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark-1.5.1/examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

# sort shuffle 32 cores, shuffle fraction 0.4
hadoop dfs -rmr /output
~/spark-1.5.1/bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 16 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf spark.shuffle.service.enabled=false --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.shuffle.memoryFraction=0.4 --conf spark.storage.memoryFraction=0.4 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.sql.tungsten.enabled=true --conf spark.driver.maxResultSize=1536 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark-1.5.1/examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

# sort shuffle 32 cores, shuffle fraction 0.2
hadoop dfs -rmr /output
~/spark-1.5.1/bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 16 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf spark.shuffle.service.enabled=false --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.shuffle.memoryFraction=0.2 --conf spark.storage.memoryFraction=0.6 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.sql.tungsten.enabled=true --conf spark.driver.maxResultSize=1536 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark-1.5.1/examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

