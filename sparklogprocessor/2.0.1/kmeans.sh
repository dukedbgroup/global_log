~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 1G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=kmeans101G,username=profiler,password=profiler,prefix=thoth-test.KMeans,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar,\
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 5 --storageLevel "MEMORY_ONLY"

~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 2G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=kmeans102G,username=profiler,password=profiler,prefix=thoth-test.KMeans,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar,\
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 5 --storageLevel "MEMORY_ONLY"

<<C1
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 4G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=kmeans104Gdisk,username=profiler,password=profiler,prefix=thoth-test.KMeans,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar,\
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 5 --storageLevel "MEMORY_AND_DISK"
C1

<<C2
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 4G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=kmeans104Gjava,username=profiler,password=profiler,prefix=thoth-test.KMeans,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar,\
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 5 --storageLevel "MEMORY_ONLY_SER"


~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.mllib.DenseKMeans \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 4G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrationRequired=true --conf spark.kryo.classesToRegister="[D,java.lang.Double,[B,org.apache.spark.mllib.linalg.DenseVector,[Ljava.lang.Object;,org.apache.spark.mllib.clustering.VectorWithNorm,[Lorg.apache.spark.mllib.clustering.VectorWithNorm;,[[Lorg.apache.spark.mllib.clustering.VectorWithNorm;,[Lscala.collection.mutable.ArrayBuffer;,scala.collection.mutable.ArrayBuffer,[Lscala.Tuple2;,scala.Tuple2,scala.collection.mutable.ArraySeq" \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=kmeans104Gkryo,username=profiler,password=profiler,prefix=thoth-test.KMeans,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar,\
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/scopt_2.11-3.5.0.jar,\
/home/mayuresh/mahout/examples/target/mahout-examples-0.13.0-SNAPSHOT-job.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/kmeans-huge/Input/samples" --k 10 --numIterations 5 --storageLevel "MEMORY_ONLY_SER"
C2
