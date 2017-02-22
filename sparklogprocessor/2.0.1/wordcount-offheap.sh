# sort shuffle 32 cores, shuffle fraction 0.6
hadoop dfs -rmr /output
~/spark/bin/spark-submit --class org.apache.spark.examples.WordCountDF --master yarn-client --num-executors 10 --driver-memory 4G --executor-memory 4G --executor-cores 4 --conf spark.memory.unsafe.offHeap=true --conf spark.memory.offHeap.size=512M --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark/examples/target/scala-2.10/spark-examples-1.6.0-SNAPSHOT-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

# sort shuffle 32 cores, shuffle fraction 0.6
hadoop dfs -rmr /output
~/spark/bin/spark-submit --class org.apache.spark.examples.WordCountDF --master yarn-client --num-executors 10 --driver-memory 4G --executor-memory 4G --executor-cores 4 --conf spark.memory.unsafe.offHeap=true --conf spark.memory.offHeap.size=1024M --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark/examples/target/scala-2.10/spark-examples-1.6.0-SNAPSHOT-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

# sort shuffle 32 cores, shuffle fraction 0.6
hadoop dfs -rmr /output
~/spark/bin/spark-submit --class org.apache.spark.examples.WordCountDF --master yarn-client --num-executors 10 --driver-memory 4G --executor-memory 4G --executor-cores 4 --conf spark.memory.unsafe.offHeap=true --conf spark.memory.offHeap.size=2048M --conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark/examples/target/scala-2.10/spark-examples-1.6.0-SNAPSHOT-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"