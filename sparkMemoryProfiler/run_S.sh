 #!/bin/bash

#$1 hadoop home directory, e.g., /home/yuzhang/hadoop-2.6.0
#$2 input file directory, e.g., /HiBench/Sort/Iniput/
#$3 output file directory, e.g., /HiBench/Sort/Output/output

# GC frequency

# Parallel GC

# NewRatio=1 2 4 8 16 64 128
#for i in 1 2 4 8 16 64 128
#do
#       for j in {1..5}
#       do
#               $1/bin/hadoop fs -rmr $3
#               ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:NewRatio=$i" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
#               sleep 10
#       done
#done

# MaxNewSize=100000000 OldSize=100000000
for j in {1..5}
do
       $1/bin/hadoop fs -rmr $3
       ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:MaxNewSize=100000000 -XX:OldSize=100000000" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
       sleep 10
done

# G1

# MaxGCPauseMillis=1 200 2000 InitiatingHeapOccupancyPercent=1 45 99

#for i1 in 1 200
#do
#       for i2 in 1 45
#       do
#               for j in {1..5}
#               do
#                      $1/bin/hadoop fs -rmr $3
#                       ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC -XX:MaxGCPauseMillis=$i1 -XX:InitiatingHeapOccupancyPercent=$i2" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
#                       sleep 10
#               done
#       done
#done
# young generation size

# MaxNewSize=400000000 800000000 1200000000 1600000000 2000000000
#for i in 1000000 10000000 100000000 200000000
#do
#       for j in {1..5}
#       do
#               $1/bin/hadoop fs -rmr $3
#               ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 6G --executor-memory 6G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:MaxNewSize=$i" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
#               sleep 10
#       done
#done




# ParallelGCThreads

# ParallelGCThreads=13 1 64
#for j in {1..5}
#do
#        $1/bin/hadoop fs -rmr $3
#        ./bin/spark-submit --class org.apache.spark.examples.SparkSort --master yarn-client --num-executors 2 --driver-memory 100G --executor-memory 100G --executor-cores 16 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
#        sleep 10
#done
#for i in 1 64
#do
#        for j in {1..5}
#        do
#                $1/bin/hadoop fs -rmr $3
#                ./bin/spark-submit --class org.apache.spark.examples.SparkSort --master yarn-client --num-executors 2 --driver-memory 100G --executor-memory 100G --executor-#cores 16 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:#+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC -XX:ParallelGCThreads=$i" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
#                sleep 10
#        done
#done

# smaller heap

for j in {1..5}
do
       $1/bin/hadoop fs -rmr $3
       ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 3G --executor-memory 3G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:MaxNewSize=100000000 -XX:OldSize=100000000" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
       sleep 10
done

for j in {1..5}
do
       $1/bin/hadoop fs -rmr $3
       ./bin/spark-submit --class org.apache.spark.examples.Sort --master yarn-client --num-executors 10 --driver-memory 3G --executor-memory 3G --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ./examples/target/scala-2.10/spark-examples-1.5.1-hadoop2.6.0.jar "$2" "$3"
       sleep 10
done



