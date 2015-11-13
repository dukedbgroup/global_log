/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.graphx

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

// han sampler import begin
import java.io._
import org.apache.spark.storage._
import java.lang.System
import java.util.Date
import java.util.concurrent._
import java.text._
import scala.util.Properties
// han sampler import end

/**
 * Driver program for running graph algorithms.
 */
object Analytics extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions> [other options]")
      System.err.println("Supported 'taskType' as follows:")
      System.err.println("  pagerank    Compute PageRank")
      System.err.println("  cc          Compute the connected components of vertices")
      System.err.println("  triangles   Count the number of triangles")
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }

    // han
    var storageLevel: StorageLevel = options.remove("storageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(null)

    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)

        // han
        //        val outFname = options.remove("output").getOrElse("")
        var outFname = options.remove("output").getOrElse("")

        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRank(" + fname + ")"))

        // han
        if (!outFname.isEmpty) {
          outFname += sc.applicationId
        }

        // han sampler 1 begin
        val SAMPLING_PERIOD: Long = 10
        val TIMESTAMP_PERIOD: Long = 1000

        var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss")

        val dirname_application = Properties.envOrElse("SPARK_HOME", "/home/yuzhang/spark-1.5.1") + "/logs/" + sc.applicationId

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

                s += "\t" + es.maxMem.toString()
                s += "\t" + es.offHeapUsed.toString()

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

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,

          // han
          //          edgeStorageLevel = edgeStorageLevel,
          //          vertexStorageLevel = vertexStorageLevel)
          edgeStorageLevel = storageLevel,
          vertexStorageLevel = storageLevel)
          .cache()

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        // han
        //        val pr = (numIterOpt match {
        //          case Some(numIter) => PageRank.run(graph, numIter)
        //          case None => PageRank.runUntilConvergence(graph, tol)
        //        }).vertices
        //                          .cache()
        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run_storageLevel(graph = graph, numIter = numIter, storageLevel = storageLevel)
          case None => PageRank.runUntilConvergence_storageLevel(graph = graph, tol = tol, storageLevel = storageLevel)
        }).vertices
          .persist(storageLevel)

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()

        // han sampler 2 begin
        f.cancel(true)
      // han sampler 2 end

      case "cc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("ConnectedComponents(" + fname + ")"))
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        val cc = ConnectedComponents.run(graph)
        println("Components: " + cc.vertices.map { case (vid, data) => data }.distinct())
        sc.stop()

      case "triangles" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("TriangleCount(" + fname + ")"))
        val graph = GraphLoader.edgeListFile(sc, fname,
          canonicalOrientation = true,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel)
          // TriangleCount requires the graph to be partitioned
          .partitionBy(partitionStrategy.getOrElse(RandomVertexCut)).cache()
        val triangles = TriangleCount.run(graph)
        println("Triangles: " + triangles.vertices.map {
          case (vid, data) => data.toLong
        }.reduce(_ + _) / 3)
        sc.stop()

      case _ =>
        println("Invalid task type.")
    }
  }
}
// scalastyle:on println
