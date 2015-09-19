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

package com.redhat.et.silex.maps.incrementing

import math.Ordering

import com.twitter.algebird.Semigroup

import com.redhat.et.silex.maps.ordered.tree._
import com.redhat.et.silex.maps.ordered.infra._

object tree {
  trait RBNodeInc[K, V] extends RBNode[K, V] {
    val valueSemigroup: Semigroup[V]

    final def increment(k: K, iv: V) = blacken(inc(k, iv))

    def inc(k: K, iv: V): RBNode[K, V]
  }

  trait LeafInc[K, V] extends RBNodeInc[K, V] with Leaf[K, V] {
    def inc(k: K, iv: V) = rNode(k, iv, this, this)
  }

  trait INodeInc[K, V] extends RBNodeInc[K, V] with INode[K, V] {
    val lsub: RBNodeInc[K, V]
    val rsub: RBNodeInc[K, V]
  }

  trait RNodeInc[K, V] extends INodeInc[K, V] with RNode[K, V] {
    def inc(k: K, iv: V) =
      if (keyOrdering.lt(k, key)) rNode(key, value, lsub.inc(k, iv), rsub)
      else if (keyOrdering.gt(k, key)) rNode(key, value, lsub, rsub.inc(k, iv))
      else rNode(key, valueSemigroup.plus(value, iv), lsub, rsub)
  }

  trait BNodeInc[K, V] extends INodeInc[K, V] with BNode[K, V] {
    def inc(k: K, iv: V) =
      if (keyOrdering.lt(k, key)) balance(bNode(key, value, lsub.inc(k, iv), rsub))
      else if (keyOrdering.gt(k, key)) balance(bNode(key, value, lsub, rsub.inc(k, iv)))
      else bNode(key, valueSemigroup.plus(value, iv), lsub, rsub)
  }
}

import tree._

object infra {
  trait IncrementingMapLike[K, V, M <: IncrementingMapLike[K, V, M]] extends
      OrderedMapLike[K, V, INodeInc[K, V], M] {

    val root: RBNodeInc[K, V]

    def increment(k: K, iv: V) = build(root.increment(k, iv))

    def valueSemigroup = root.valueSemigroup
  }
}

import infra._

case class IncrementingMap[K, V](root: RBNodeInc[K, V]) extends
    IncrementingMapLike[K, V, IncrementingMap[K, V]] {

  def build(n: RBNode[K, V]) = IncrementingMap(n.asInstanceOf[RBNodeInc[K, V]])

  override def toString =
    "IncrementingMap(" +
      nodesIterator.map(n => s"${n.key} -> ${n.value}").mkString(", ") +
    ")"
}

object IncrementingMap {
  class Reify[K, V](val keyOrdering: Ordering[K], val valueSemigroup: Semigroup[V]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Reify[K, V](keyOrdering, valueSemigroup) with RNodeInc[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeInc[K, V]]
        val rsub = rs.asInstanceOf[RBNodeInc[K, V]]
      }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Reify[K, V](keyOrdering, valueSemigroup) with BNodeInc[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeInc[K, V]]
        val rsub = rs.asInstanceOf[RBNodeInc[K, V]]
      }
  }

  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V](implicit vsg: Semigroup[V]) =
      IncrementingMap(new Reify[K, V](ord, vsg) with LeafInc[K, V])
  }
}
