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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._

// han import begin
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.MutableList
// han import end

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
 *
 * The first implementation uses the standalone [[Graph]] interface and runs PageRank
 * for a fixed number of iterations:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the [[Pregel]] interface and runs PageRank until
 * convergence:
 *
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 0.0 )
 * while( max(abs(PR - oldPr)) > tol ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
 *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
 * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * Note that this is not the "normalized" PageRank and as a consequence pages that have no
 * inlinks will have a PageRank of alpha.
 */
object PageRank extends Logging {

  // han
  private var storageLevel: StorageLevel = null

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] =
    {
      runWithOptions(graph, numIter, resetProb)
    }

  // han
  def run_storageLevel[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
    resetProb: Double = 0.15, storageLevel: StorageLevel): Graph[Double, Double] =
    {
      this.storageLevel = storageLevel
      run(graph, numIter, resetProb)
    }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
    srcId: Option[VertexId] = None): Graph[Double, Double] =
    {
      // Initialize the PageRank graph with each edge attribute having
      // weight 1/outDegree and each vertex with attribute 1.0.
      var rankGraph: Graph[Double, Double] = graph
        // Associate the degree with each vertex
        .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
        // Set the weight on the edges based on the degree
        .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
        // Set the vertex attributes to the initial pagerank values
        .mapVertices((id, attr) => resetProb)

      val personalized = srcId isDefined
      val src: VertexId = srcId.getOrElse(-1L)
      def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

      var iteration = 0
      var prevRankGraph: Graph[Double, Double] = null

      // han      
      val iterationStartTime = System.nanoTime()
      println("Iteration 1 starts:\n" + iterationStartTime)
      var iDurs: MutableList[(Int, Double)] = new MutableList[(Int, Double)]
      var iterationEndTime_last: Long = iterationStartTime

      while (iteration < numIter) {
        rankGraph
          // han          
          //.cache()
          .persist(storageLevel)

        // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
        // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
        val rankUpdates = rankGraph.aggregateMessages[Double](
          ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

        // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
        // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
        // edge partitions.
        prevRankGraph = rankGraph
        val rPrb = if (personalized) {
          (src: VertexId, id: VertexId) => resetProb * delta(src, id)
        } else {
          (src: VertexId, id: VertexId) => resetProb
        }

        rankGraph = rankGraph.joinVertices(rankUpdates) {
          (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
        }
          // han
          //.cache()
          .persist(storageLevel)

        rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
        logInfo(s"PageRank finished iteration $iteration.")

        prevRankGraph.vertices.unpersist(false)
        prevRankGraph.edges.unpersist(false)

        // han
        val iterationEndTime = System.nanoTime()
        var iDuration: Double = (iterationEndTime - iterationEndTime_last) / 1000000000.0
        iDurs += ((iteration, iDuration))
        println("Iteration " + iteration + ": " + iDuration)
        iterationEndTime_last = iterationEndTime

        iteration += 1
      }

      // han
      println("Iterations count:\n" + iteration)
      println("Iterations complete:\n" + System.nanoTime())
      println("Iterations:")
      iDurs.foreach {
        id =>
          println("Iteration " + id._1 + ": " + id._2)
      }

      rankGraph
    }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
    {
      runUntilConvergenceWithOptions(graph, tol, resetProb)
    }

  // han
  def runUntilConvergence_storageLevel[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15, storageLevel: StorageLevel): Graph[Double, Double] =
    {
      this.storageLevel = storageLevel
      runUntilConvergence(graph, tol, resetProb)
    }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */

  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
    srcId: Option[VertexId] = None): Graph[Double, Double] =
    {

      // Initialize the pagerankGraph with each edge attribute
      // having weight 1/outDegree and each vertex with attribute 1.0.
      val pagerankGraph: Graph[(Double, Double), Double] = graph
        // Associate the degree with each vertex
        .outerJoinVertices(graph.outDegrees) {
          (vid, vdata, deg) => deg.getOrElse(0)
        }
        // Set the weight on the edges based on the degree
        .mapTriplets(e => 1.0 / e.srcAttr)
        // Set the vertex attributes to (initalPR, delta = 0)
        .mapVertices((id, attr) => (0.0, 0.0))
        // han
        //        .cache()
        .persist(storageLevel)

      val personalized = srcId.isDefined
      val src: VertexId = srcId.getOrElse(-1L)

      // Define the three functions needed to implement PageRank in the GraphX
      // version of Pregel
      def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
        val (oldPR, lastDelta) = attr
        val newPR = oldPR + (1.0 - resetProb) * msgSum
        (newPR, newPR - oldPR)
      }

      def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
        msgSum: Double): (Double, Double) = {
        val (oldPR, lastDelta) = attr
        var teleport = oldPR
        val delta = if (src == id) 1.0 else 0.0
        teleport = oldPR * delta

        val newPR = teleport + (1.0 - resetProb) * msgSum
        (newPR, newPR - oldPR)
      }

      def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
        if (edge.srcAttr._2 > tol) {
          Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
        } else {
          Iterator.empty
        }
      }

      def messageCombiner(a: Double, b: Double): Double = a + b

      // The initial message received by all vertices in PageRank
      val initialMessage = resetProb / (1.0 - resetProb)

      // Execute a dynamic version of Pregel.
      val vp = if (personalized) {
        (id: VertexId, attr: (Double, Double), msgSum: Double) =>
          personalizedVertexProgram(id, attr, msgSum)
      } else {
        (id: VertexId, attr: (Double, Double), msgSum: Double) =>
          vertexProgram(id, attr, msgSum)
      }

      Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
        vp, sendMessage, messageCombiner)
        .mapVertices((vid, attr) => attr._1)
    } // end of deltaPageRank

}
