Function: 
Sample memory usage of a running Spark application.

Compatibility:
Spark-1.5.1, YARN client mode

Installation:
1. Copy the folders and files into corresponding Spark source folders on the driver node
2. set $SPARK_HOME to the installation directiory of Spark on every driver and worker node
3. rebuild Spark on the driver node:
cd $SPARK_HOME
build/sbt -Pnetlib-lgpl -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests assembly


