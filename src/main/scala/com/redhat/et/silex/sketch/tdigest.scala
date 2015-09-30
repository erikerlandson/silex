/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.c
 */

package com.redhat.et.silex.sketch.tdigest

import map.TDigestMap

/** A t-digest object */
class TDigest(
  val delta: Double,
  val K: Double,
  val n: Long,
  val nclusters: Int,
  val clusters: TDigestMap[Double, Double, Double]) {

  private case class Cluster(centroid: Double, mass: Double, massUB: Double)

  /** Returns a new t-digest with new pair (x, w) included in its sketch.
    * This implements 'algorithm 1' from:
    * Computing Extremely Accurate Quantiles Using t-Digests
    * Ted Dunning and Otmar Ertl
    * https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
    */
  def +[N1, N2](xw: (N1, N2))
      (implicit num1: Numeric[N1], num2: Numeric[N2]): TDigest = {
    val xn = num1.toDouble(xw._1)
    var wn = num2.toDouble(xw._2)
    require(wn > 0.0, "data weight must be > 0")

    val nn = n + 1L

    // Get the current cluster nearest to incoming (xn)
    // Note: 'near' will have length 0,1, or 2:
    // length 0 => current cluster map was empty (no data yet)
    // length 1 => exactly one cluster was closest to (xn)
    // length 2 => (xn) was mid-point between two clusters (both are returned, in key order)
    val near = clusters.nearest(xn)

    if (near.isEmpty) {
      // our map is empty, so insert this pair as the first cluster
      new TDigest(delta, K, nn, nclusters + 1, clusters + ((xn, wn)))
    } else {
      // compute upper bounds for cluster masses, from their quantile estimates
      var massPS = clusters.prefixSum(near.head._1, open=true)
      val massTotal = clusters.sum
      val s = near.map { case (c, m) =>
        val q = (massPS + m / 2.0) / massTotal
        val ub = 4.0 * nn.toDouble * delta * q * (1.0 - q)
        massPS += m
        Cluster(c, m, ub)
      }

      // assign new mass (wn) among the clusters
      var cmNew = Vector.empty[(Double, Double)]
      scala.util.Random.shuffle(s).foreach { clust =>
        if (wn <= 0.0) {
          // if we have already distributed all the mass, remaining clusters unchanged
          cmNew = cmNew :+ ((clust.centroid, clust.mass))
        } else if (xn == clust.centroid) {
          // if xn lies exactly on the centroid, add all mass in regardless of bound
          cmNew = cmNew :+ ((clust.centroid, clust.mass + wn))
          wn = 0.0
        } else if (clust.mass < clust.massUB) {
          // cluster can accept more mass, respecting its upper bound
          val dm = math.min(wn, clust.massUB - clust.mass)
          val mass = clust.mass + dm
          val dc = dm * (xn - clust.centroid) / mass
          wn -= dm
          cmNew = cmNew :+ ((clust.centroid + dc, mass))
        } else {
          // cluster is at its upper bound for mass, it remains unchanged
          cmNew = cmNew :+ ((clust.centroid, clust.mass))
        }
      }

      // any remaining mass becomes a new cluster
      if (wn > 0.0) cmNew = cmNew :+ ((xn, wn))

      // remove original clusters and replace with the new ones
      val clustDel = near.iterator.map(_._1).foldLeft(clusters)((c, e) => c - e)
      val clustNew = cmNew.foldLeft(clustDel)((c, p) => c.increment(p._1, p._2))
      val nc = nclusters - s.length + cmNew.length

      if (nc.toDouble <= K / delta)
        // return the updated t-digest
        new TDigest(delta, K, nn, nc, clustNew)
      else {
        // too many clusters: compress it by re-clustering
        val ds = scala.util.Random.shuffle(clustNew.toVector)
        ds.foldLeft(TDigest.empty(1.0 / delta, K))((c, e) => c + e)
      }
    }
  }
}

object TDigest {
  import scala.language.reflectiveCalls
  import com.twitter.algebird.Monoid
  import com.redhat.et.silex.maps.prefixsum.IncrementingMonoid

  /** return an empty t-digest */
  def empty(
    deltaInv: Double = 100.0,
    K: Double = 2.0) = {
    require(deltaInv >= 1.0, s"deltaInv= $deltaInv")
    require(K >= 1.0, s"K= $K")
    val cmap = TDigestMap.key[Double].value[Double].prefix(IncrementingMonoid.fromMonoid[Double])
    new TDigest(1.0 / deltaInv, K, 0L, 0, cmap)
  }

  /** return a t-digest constructed from some data */
  def apply[N](
    data: TraversableOnce[N],
    deltaInv: Double = 100.0,
    K: Double = 2.0)(implicit num: Numeric[N]) = {
    require(deltaInv >= 1.0, s"deltaInv= $deltaInv")
    require(K >= 1.0, s"K= $K")
    val td = data.foldLeft(empty(deltaInv, K))((c, e) => c + ((e, 1)))
    val ds = scala.util.Random.shuffle(td.clusters.toVector)
    ds.foldLeft(empty(deltaInv, K))((c, e) => c + e)
  }
}
