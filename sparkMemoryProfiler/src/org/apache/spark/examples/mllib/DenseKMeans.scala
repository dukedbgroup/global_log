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
package org.apache.spark.examples.mllib

import org.apache.log4j.{ Level, Logger }
import scopt.OptionParser

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors

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
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
    input: String = null,
    k: Int = -1,
    numIterations: Int = 10,
    initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
          s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DenseKMeans with $params")
    val sc = new SparkContext(conf)

    // han sampler 1 begin
    val SAMPLING_PERIOD: Long = 500
    val TIMESTAMP_PERIOD: Long = 1000

    var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss a")

    val dirname_application = Properties.envOrElse("SPARK_HOME", "/home/ubuntu/spark-1.5.1") + "/logs/" + sc.applicationId
    val dir_application = new File(dirname_application)
    if (!dir_application.exists())
      dir_application.mkdirs()

    //    val writer = new FileWriter(new File("/home/ubuntu/sparkOutput/sparkOutput_driver_" + System.nanoTime() + ".txt"), true)
    val writer = new FileWriter(new File(dirname_application + "/sparkOutput_driver_" + sc.applicationId + ".txt"), true)
    writer.write("Storage memory\n")

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      var i: Long = 0
      override def run {
        var statusArray = sc.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))
        if (statusArray.size >= 1) {

          var value = statusArray(0).memUsed
          //println(value)
          var s = value.toString()
          if (i % TIMESTAMP_PERIOD == 0) {
            i = 0
            var time: String = dateFormat.format(new Date())
            s += "\t" + time
          }
          i = i + SAMPLING_PERIOD
          writer.write(s + "\n")
          writer.flush()
        }
      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, SAMPLING_PERIOD, TimeUnit.MILLISECONDS)
    // han sampler 1 end 

    //    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = sc.textFile(params.input).map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .run(examples)

    val cost = model.computeCost(examples)

    println(s"Total cost = $cost.")

    // han
    // Save and load model
    val outputPath = "/kmeans/output/" + sc.applicationId
    model.save(sc, outputPath)
    val sameModel = KMeansModel.load(sc, outputPath)

    sc.stop()

    // han sampler 2 begin
    f.cancel(true)
    writer.flush()
    writer.close()
    // hand sampler 2 end
  }
}
// scalastyle:on println

