package org.apache.spark.examples

import scala.util.Random
import scala.collection.mutable

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io._
import org.apache.spark.storage._
import java.lang.System
import java.util._
import java.util.concurrent._

object RandomTextSearch {

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("SparkTextSearch [inputFilename] [numberOfSearchKeys]");
      return
    }

    val sparkConf = new SparkConf().setAppName("RandomTextSearch $args")
    val sc = new SparkContext(sparkConf)

    // han sampler 1 begin
    val SAMPLING_PERIOD: Long = 10
    val TIMESTAMP_PERIOD: Long = 1000

    var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss")

    val dirname_application = Properties.envOrElse("SPARK_HOME", "/home/ubuntu/spark-1.5.1") + "/logs/" + sc.applicationId
    val dir_application = new File(dirname_application)
    if (!dir_application.exists())
      dir_application.mkdirs()

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      var i: Long = 0
      override def run {

        //        sc.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))
        sc.getExecutorStorageStatus.foreach {
          es =>
            val filename: String = dirname_application + "/sparkOutput_driver_" + sc.applicationId + "_" + es.blockManagerId + ".txt"
            val file = new File(filename)
            val writer = new FileWriter(file, true)
            if (!file.exists()) {
              file.createNewFile()
              writer.write(sc.applicationId + "_" + es.blockManagerId + "\n")
              writer.flush()
              writer.close()
            }
            var s = es.memUsed.toString()
            //println(s)
            if (i % TIMESTAMP_PERIOD == 0) {
              i = 0
              var time: String = dateFormat.format(new Date())
              s += "\t" + time
            }

            writer.write(s + "\n")
            writer.flush()
            writer.close()
        }
        i = i + SAMPLING_PERIOD
      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, SAMPLING_PERIOD, TimeUnit.MILLISECONDS)
    // han sampler 1 end 

    val file = sc.textFile(args(0), 1)
    val n = args(1).toInt

    val op = file.filter(l => { val lookFor=new scala.collection.mutable.ListBuffer[String]; for(i <- 1 to n) {lookFor+=scala.util.Random.alphanumeric.take(5).mkString;}; var res=false; var s=""; for(s <- lookFor){if(l.contains(s)){res=true}}; res })

    println("Num matches: " + op.count());

    f.cancel(true)
    spark.stop()
  }
}
