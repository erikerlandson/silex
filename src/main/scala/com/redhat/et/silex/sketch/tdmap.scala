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

package com.redhat.et.silex.sketch.tdigest.map

import math.Numeric

import com.twitter.algebird.Monoid

import com.redhat.et.silex.maps.increment._
import com.redhat.et.silex.maps.prefixsum._
import com.redhat.et.silex.maps.nearest._

object tree {
  import com.redhat.et.silex.maps.increment.tree._
  import com.redhat.et.silex.maps.prefixsum.tree._
  import com.redhat.et.silex.maps.nearest.tree._

  trait NodeTD extends NodePS[Double, Double, Double]
      with NodeInc[Double, Double] with NodeNearMap[Double, Double] {
  }

  trait LNodeTD extends NodeTD
      with LNodePS[Double, Double, Double] with LNodeInc[Double, Double]
      with LNodeNearMap[Double, Double] {
  }

  trait INodeTD extends NodeTD
      with INodePS[Double, Double, Double] with INodeInc[Double, Double]
      with INodeNearMap[Double, Double] {
    val lsub: NodeTD
    val rsub: NodeTD
  }
}

import tree._

object infra {
  import com.redhat.et.silex.maps.redblack.tree._
  import com.redhat.et.silex.maps.ordered.tree.DataMap

  class Inject {
    // I want to fix the typeclasses corresponding to "regular real numbers" here:
    val keyOrdering = implicitly[Numeric[Double]]
    val valueMonoid = implicitly[Monoid[Double]]
    val prefixMonoid = IncrementingMonoid.fromMonoid[Double]

    def iNode(clr: Color, dat: Data[Double], ls: Node[Double], rs: Node[Double]) =
      new Inject with INodeTD with TDigestMap {
        // INode
        val color = clr
        val lsub = ls.asInstanceOf[NodeTD]
        val rsub = rs.asInstanceOf[NodeTD]
        val data = dat.asInstanceOf[DataMap[Double, Double]]
        // INodePS
        val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), data.value)
        // INodeNear
        val kmin = lsub match {
          case n: INodeTD => n.kmin
          case _ => data.key
        }
        val kmax = rsub match {
          case n: INodeTD => n.kmax
          case _ => data.key
        }
      }
  }

}

import infra._

sealed trait TDigestMap
  extends IncrementMapLike[Double, Double, INodeTD, TDigestMap]
  with PrefixSumMapLike[Double, Double, Double, INodeTD, TDigestMap]
  with NearestMapLike[Double, Double, INodeTD, TDigestMap] {

  def cdf[N](xx: N)(implicit num: Numeric[N]) = {
    val x = num.toDouble(xx)
    val adj = this.adjacent(x)
    adj.length match {
      case 0 => 0.0
      case 1 => {
        val (c, m) = adj(0)
        // query x is either to the left of all clusters, or to the right of them,
        // or is equal to one of them
        if (x < c) 0.0
        else if (x > c) 1.0
        else {
          val psum =
            if (c == this.keyMax.get) this.prefixSum(c)
            else this.prefixSum(c, open = true) + (m / 2.0)
          psum / this.sum
        }
      }
      case 2 => {
        val (c1, tm1) = adj(0)
        val (c2, tm2) = adj(1)
        // query x is strictly between c1 and c2
        val psum = this.prefixSum(c1, open = true)
        val m1 = tm1 / 2.0
        val m2 = (tm1 / 2.0) + (if (c2 == this.keyMax.get) tm2 else tm2 / 2.0)
        val a = (x - c1) / (c2 - c1)
        (psum + m1 + a * m2) / this.sum
      }
      case _ => throw new Exception(s"undefined adjacent return = $adj")
    }
  }

  override def toString =
    "TDigestMap(" +
      iterator.zip(prefixSumsIterator())
        .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
    ")"
}

object TDigestMap {
  def empty = new Inject with LNodeTD with TDigestMap
}
