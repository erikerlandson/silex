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
    this.coverR(x) match {
      case Cover(Some((c1, tm1)), Some((c2, tm2))) => {
        val psum = this.prefixSum(c1, open = true)
        val (m1, t) = if (c1 == this.keyMin.get) (0.0, tm1) else (tm1 / 2.0, tm1 / 2.0)
        val m2 = t + (if (c2 == this.keyMax.get) tm2 else tm2 / 2.0)
        val a = (x - c1) / (c2 - c1)
        (psum + m1 + a * m2) / this.sum
      }
      case Cover(Some(_), None) => 1.0
      case _ => 0.0
    }
  }

  def pdf[N](xx: N)(implicit num: Numeric[N]) = {
    val x = num.toDouble(xx)
    this.coverR(x) match {
      case Cover(Some((c1, tm1)), Some((c2, tm2))) => {
        val (m1, t) = if (c1 == this.keyMin.get) (0.0, tm1) else (tm1 / 2.0, tm1 / 2.0)
        val m2 = t + (if (c2 == this.keyMax.get) tm2 else tm2 / 2.0)
        m2 / this.sum / (c2 - c1)
      }
      case _ => 0.0
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
