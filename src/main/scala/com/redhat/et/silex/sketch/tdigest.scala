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
  val clusters: TDigestMap[Double, Vector[Double], Double]) {

  private case class Cluster(centroid: Double, mass: Double, massUB: Double)

  /** Returns a new t-digest with (x, w) included in its sketch */
  def +[N1, N2](xw: (N1, N2))
      (implicit num1: Numeric[N1], num2: Numeric[N2]): TDigest = {
    val xn = num1.toDouble(xw._1)
    var wn = num2.toDouble(xw._2)
    require(wn > 0.0, "data weight must be > 0")

    val nn = n + 1L
    val near = clusters.nearest(xn)
    if (near.isEmpty) {
      // our map is empty, so insert this pair as the first cluster
      new TDigest(delta, K, nn, nclusters + 1, clusters + ((xn, Vector(wn))))
    } else {
      // unpack into centroid/mass pairs 
      val cmPairs = TDigest.flatItr(near).toVector.sortBy(_._1)

      // compute upper bounds for cluster masses, from their quantile estimates
      var massPS = clusters.prefixSum(cmPairs.head._1, open=true)
      val massTotal = clusters.sum
      val s = cmPairs.map { case (c, m) =>
        val q = (massPS + m / 2.0) / massTotal
        val ub = 4.0 * nn.toDouble * delta * q * (1.0 - q)
        massPS += m
        Cluster(c, m, ub)
      }

      // assign new mass (wn) among the clusters
      var cmNew = Vector.empty[(Double, Double)]
      scala.util.Random.shuffle(s).foreach { clust =>
        if ((wn > 0.0) && (clust.mass < clust.massUB)) {
          val dm = math.min(wn, clust.massUB - clust.mass)
          val mass = clust.mass + dm
          val dc = dm * (xn - clust.centroid) / mass
          wn -= dm
          cmNew = cmNew :+ ((clust.centroid + dc, mass))
        } else {
          cmNew = cmNew :+ ((clust.centroid, clust.mass))
        }
      }

      // any remaining mass becomes a new cluster
      if (wn > 0.0) cmNew = cmNew :+ ((xn, wn))

      // remove original clusters and replace with the new ones
      val clustDel = near.iterator.map(_._1).foldLeft(clusters)((c, e) => c - e)
      val clustNew = cmNew.foldLeft(clustDel)((c, p) => c.increment(p._1, Vector(p._2)))
      val nc = nclusters - s.length + cmNew.length

      if (nc.toDouble <= K / delta)
        // return the updated t-digest
        new TDigest(delta, K, nn, nc, clustNew)
      else {
        // too many clusters: compress it by re-clustering
        val ds = scala.util.Random.shuffle(TDigest.flatItr(clustNew).toVector)
        ds.foldLeft(TDigest.empty(1.0 / delta, K))((c, e) => c + e)
      }
    }
  }
}

object TDigest {
  import scala.language.reflectiveCalls
  import com.twitter.algebird.Monoid
  import com.redhat.et.silex.maps.prefixsum.IncrementingMonoid

  private object clusterMonoid extends Monoid[Vector[Double]] {
    val zero = Vector.empty[Double]
    def plus(v1: Vector[Double], v2: Vector[Double]) = v1 ++ v2
  }

  private object prefixMonoid extends IncrementingMonoid[Double, Vector[Double]] {
    val zero = 0.0
    def plus(ps1: Double, ps2: Double) = ps1 + ps2
    def inc(ps: Double, v: Vector[Double]) = v.foldLeft(ps)(_ + _)
  }

  private[tdigest] def flatItr(ip: => Iterable[(Double, Vector[Double])]) =
    ip.iterator.flatMap { case (c, v) =>
      v.iterator.map(e => (c, e))
    }

  def empty(
    deltaInv: Double = 100.0,
    K: Double = 2.0) = {
    require(deltaInv >= 1.0, s"deltaInv= $deltaInv")
    require(K >= 1.0, s"K= $K")
    val cmap = TDigestMap.key[Double].value(clusterMonoid).prefix(prefixMonoid)
    new TDigest(1.0 / deltaInv, K, 0L, 0, cmap)
  }

  def apply[N](
    data: TraversableOnce[N],
    deltaInv: Double = 100.0,
    K: Double = 2.0)(implicit num: Numeric[N]) = {
    require(deltaInv >= 1.0, s"deltaInv= $deltaInv")
    require(K >= 1.0, s"K= $K")
    val td = data.foldLeft(empty(deltaInv, K))((c, e) => c + ((e, 1)))
    val ds = scala.util.Random.shuffle(flatItr(td.clusters).toVector)
    ds.foldLeft(empty(deltaInv, K))((c, e) => c + e)
  }
}
