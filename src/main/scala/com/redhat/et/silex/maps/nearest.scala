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

package com.redhat.et.silex.maps.nearest

import math.Numeric

import com.redhat.et.silex.maps.ordered.tree._
import com.redhat.et.silex.maps.ordered.infra._

object tree {
  trait RBNodeNear[K, V] extends RBNode[K, V] {
    val keyOrdering: Numeric[K] // <: Ordering[K]

    def nearest(k: K): Set[(K, V)]

    final def dist(k1: K, k2: K) = keyOrdering.abs(keyOrdering.minus(k1, k2))
  }

  trait LeafNear[K, V] extends RBNodeNear[K, V] with Leaf[K, V] {
    def nearest(k: K) = Set.empty[(K, V)]
  }

  trait INodeNear[K, V] extends RBNodeNear[K, V] with INode[K, V] {
    val lsub: RBNodeNear[K, V]
    val rsub: RBNodeNear[K, V]

    val kmin: K
    val kmax: K

    def nearest(k: K) = {
      if (keyOrdering.lt(k, key)) {
        lsub match {
          case ls: INodeNear[K, V] => {
            if (keyOrdering.lteq(k, ls.kmax)) ls.nearest(k)
            else {
              val (dk, ldk) = (dist(k, key), dist(k, ls.kmax))
              if (keyOrdering.lt(dk, ldk)) Set(((key, value)))
              else if (keyOrdering.gt(dk, ldk)) Set(((ls.kmax, ls.node(ls.kmax).get.value)))
              else Set(((key, value)), ((ls.kmax, ls.node(ls.kmax).get.value)))
            }
          }
          case _ => Set(((key, value)))
        }
      } else if (keyOrdering.gt(k, key)) {
        rsub match {
          case rs: INodeNear[K, V] => {
            if (keyOrdering.gteq(k, rs.kmin)) rs.nearest(k)
            else {
              val (dk, rdk) = (dist(k, key), dist(k, rs.kmin))
              if (keyOrdering.lt(dk, rdk)) Set(((key, value)))
              else if (keyOrdering.gt(dk, rdk)) Set(((rs.kmin, rs.node(rs.kmin).get.value)))
              else Set(((key, value)), ((rs.kmin, rs.node(rs.kmin).get.value)))
            }
          }
          case _ => Set(((key, value)))
        }
      } else Set(((key, value)))
    }

    override def toString = s"INodeNear($key, $value, $kmin, $kmax)"
  }

  trait RNodeNear[K, V] extends INodeNear[K, V] with RNode[K, V]
  trait BNodeNear[K, V] extends INodeNear[K, V] with BNode[K, V]
}

import tree._

object infra {
  trait NearestMapLike[K, V, M <: NearestMapLike[K, V, M]] extends
      OrderedMapLike[K, V, INodeNear[K, V], M] {

    val root: RBNodeNear[K, V]

    def nearest(k: K) = root.nearest(k)
  }
}

import infra._

case class NearestMap[K, V](root: RBNodeNear[K, V]) extends
    NearestMapLike[K, V, NearestMap[K, V]] {

  def build(n: RBNode[K, V]) = NearestMap(n.asInstanceOf[RBNodeNear[K, V]])

  override def toString =
    "NearestMap(" +
      nodesIterator.map(n => s"${n.key} -> ${n.value}").mkString(", ") +
    ")"
}

object NearestMap {
  class Reify[K, V](val keyOrdering: Numeric[K]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Reify[K, V](keyOrdering) with RNodeNear[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeNear[K, V]]
        val rsub = rs.asInstanceOf[RBNodeNear[K, V]]
        val kmin = lsub match {
          case n: INodeNear[K, V] => n.kmin
          case _ => key
        }
        val kmax = rsub match {
          case n: INodeNear[K, V] => n.kmax
          case _ => key
        }
      }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Reify[K, V](keyOrdering) with BNodeNear[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeNear[K, V]]
        val rsub = rs.asInstanceOf[RBNodeNear[K, V]]
        val kmin = lsub match {
          case n: INodeNear[K, V] => n.kmin
          case _ => key
        }
        val kmax = rsub match {
          case n: INodeNear[K, V] => n.kmax
          case _ => key
        }
      }
  }

  def key[K](implicit num: Numeric[K]) = new AnyRef {
    def value[V] =
      NearestMap(new Reify[K, V](num) with LeafNear[K, V])
  }
}
