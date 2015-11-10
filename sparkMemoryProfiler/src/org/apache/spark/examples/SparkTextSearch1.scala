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

/**
 * Transitive closure on a graph.
 */
object SparkTextSearch1 {

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("SparkTextSearch [numberOfPartitions] [inputFilename] [numberOfFiles] [numberOfSearchesPerFile] [numberOfFilesToCache] [searchKey]*");
      return
    }

    val sparkConf = new SparkConf().setAppName("SparkTextSearch")
    val spark = new SparkContext(sparkConf)

    val writer = new FileWriter(new File("/home/ubuntu/sparkOutput/sparkOutput_" + System.nanoTime() + ".txt"), true)

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      var i = 0
      override def run {
        var statusArray = spark.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))
        if (statusArray.size >= 1) {

          var value = statusArray(0).memUsed
          //println(value)
          writer.write(value + "\n")
          writer.flush()
          if (i % 1000 == 0) {
            i = 0
            writer.write(new Date() + "\n")
            writer.flush()
          }
          i = i + 100
        }
      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, 100, TimeUnit.MILLISECONDS)
    val numberOfPartitions = if (args.length > 0) args(0).toInt else 1
    val inputFilename = if (args.length > 1) args(1).toString else "/lineitem.tbl"
    val numberOfFiles = if (args.length > 2) args(2).toInt else 1
    val numberOfSearchesPerFile = if (args.length > 3) args(3).toInt else 1
    var numberOfFilesToCache = if (args.length > 4) args(4).toInt else numberOfFiles

    var searchKeys = new Array[String](7)
    searchKeys(0) = if (args.length > 5) args(5).toString else "REG AIR"
    searchKeys(1) = if (args.length > 6) args(6).toString else "DELIVER IN PERSON"
    searchKeys(2) = if (args.length > 7) args(7).toString else "COLLECT COD"
    searchKeys(3) = if (args.length > 8) args(8).toString else "TAKE BACK RETURN"
    searchKeys(4) = if (args.length > 9) args(9).toString else "FOB"
    searchKeys(5) = if (args.length > 10) args(10).toString else "RAIL"
    searchKeys(6) = if (args.length > 11) args(11).toString else "TRUCK"

    var textFiles = new Array[RDD[String]](numberOfFiles)
    var numberOfCompletedSearches = 0

    for (j <- 0 to numberOfFiles - 1) {
      println("File " + j + ":")

      textFiles(j) = spark.textFile(inputFilename, numberOfPartitions).filter(line => line.contains("A"))
      if (numberOfFilesToCache > 0) {
        textFiles(j).cache()
        println("RDDs cached")
        numberOfFilesToCache = numberOfFilesToCache - 1
      }

      var count: Long = 0
      var i = 0;
      for (i <- 0 to numberOfSearchesPerFile - 1) {
        var index = i % searchKeys.length
        count = textFiles(j).filter(line => line.contains(searchKeys(index))).count()
        println(searchKeys(index) + ": " + count + " lines")

/*
        count = 0
        textFiles(j).toArray.foreach {
          line => count = count + line.split("\\||,").filter(token => token.contains(searchKeys(index))).size
          //println("+++" + line)
        }
        println(searchKeys(index) + ": " + count + " times")
*/

        //      textFiles(j).filter(line => line.contains(searchKeys(index))).collect()
        numberOfCompletedSearches = numberOfCompletedSearches + 1
        println("Completed searches: " + numberOfCompletedSearches + "/" + numberOfFiles * numberOfSearchesPerFile)
      }
      /*
//      val writer = new FileWriter(new File("/home/ubuntu/sparkOutput.txt"), true)
      val st1 = spark.getExecutorStorageStatus
      println("spark.getExecutorStorageStatus")
      println(st1.size)
      println("memUsed: " + st1(0).memUsed)
      println("offHeapUsed: " + st1(0).offHeapUsed)
      println("memUsed: " + st1(1).memUsed)
      println("offHeapUsed: " + st1(1).offHeapUsed)



      val st2 = spark.getExecutorMemoryStatus
      println("spark.getExecutorStorageStatus")
      println(st2.size)
      println(st2)

//      writer.write(st1(0).blockManagerId.host + "\t" + st1(1).blockManagerId.host + "\n")
 //     writer.write(st1(0).memUsed + "\t" + st1(1).memUsed + "\n")
//      var value = spark.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))(0).memUsed
//      writer.write(value + "\n")
//      writer.close()
*/
    }

    f.cancel(true)
    writer.flush()
    writer.close()
    spark.stop()
  }
}
