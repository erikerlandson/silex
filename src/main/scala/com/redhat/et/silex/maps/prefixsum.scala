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

package com.redhat.et.silex.maps.prefixsum

import math.Ordering

import com.twitter.algebird.{ Semigroup, Monoid, MonoidAggregator }

import com.redhat.et.silex.maps.ordered.tree._
import com.redhat.et.silex.maps.ordered.infra._

object tree {
  trait RBNodePS[K, V, P] extends RBNode[K, V] {
    val prefixMonoid: IncrementingMonoid[P, V]

    final def prefixSum(k: K, open: Boolean = false) = pfSum(k, prefixMonoid.zero, open)

    def pfSum(k: K, sum: P, open: Boolean): P
    def pfs: P
  }

  trait LeafPS[K, V, P] extends RBNodePS[K, V, P] with Leaf[K, V] {
    def pfSum(k: K, sum: P, open: Boolean) = sum
    def pfs = prefixMonoid.zero  
  }

  trait INodePS[K, V, P] extends RBNodePS[K, V, P] with INode[K, V] {
    val lsub: RBNodePS[K, V, P]
    val rsub: RBNodePS[K, V, P]

    val prefix: P

    def pfSum(k: K, sum: P, open: Boolean) =
      if (keyOrdering.lt(k, key)) lsub.pfSum(k, sum, open)
      else if (keyOrdering.gt(k, key)) rsub.pfSum(k, prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), value), open)
      else if (open) prefixMonoid.plus(sum, lsub.pfs)
      else prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), value)

    def pfs = prefix

    override def toString = s"INodePS($key,$value,$prefix)"
  }

  trait RNodePS[K, V, P] extends INodePS[K, V, P] with RNode[K, V]
  trait BNodePS[K, V, P] extends INodePS[K, V, P] with BNode[K, V]
}

import com.redhat.et.silex.maps.prefixsum.tree._

object infra {
  trait PrefixSumMapLike[K, V, P, M <: PrefixSumMapLike[K, V, P, M]] extends OrderedMapLike[K, V, INodePS[K, V, P], M] {
    val root: RBNodePS[K, V, P]

    def prefixSum(k: K, open: Boolean = false) = root.prefixSum(k, open)

    def prefixSums(open: Boolean = false) = prefixSumsIterator(open).toIterable

    def prefixSumsIterator(open: Boolean = false) = {
      val mon = root.prefixMonoid
      val itr = valuesIterator.scanLeft(mon.zero)((p,e)=>mon.inc(p,e))
      if (open) itr.takeWhile(_ => itr.hasNext) else itr.drop(1)
    }

    def prefixMonoid = root.prefixMonoid
  }
}

trait IncrementingMonoid[T, E] extends Monoid[T] {
  def inc(t: T, e: E): T
}

object IncrementingMonoid {
  def fromMonoidAggregator[T, E](agg: MonoidAggregator[E, T, T]) = new IncrementingMonoid[T, E] {
    def zero = agg.monoid.zero
    def plus(l: T, r: T) = agg.monoid.plus(l, r)
    def inc(t: T, e: E) = agg.monoid.plus(t, agg.prepare(e))
  }
  def fromMonoid[T](implicit monoid: Monoid[T]) = new IncrementingMonoid[T, T] {
    def zero = monoid.zero
    def plus(l: T, r: T) = monoid.plus(l, r)
    def inc(t: T, e: T) = monoid.plus(t, e)
  }
  def from[T, E](z: T, p: (T, T) => T, i: (T, E) => T): IncrementingMonoid[T, E] = new IncrementingMonoid[T, E] {
    def zero = z
    def plus(l: T, r: T) = p(l, r)
    def inc(t: T, e: E) = i(t, e)
  }
  def from[T](z: T)(p: (T, T) => T): IncrementingMonoid[T, T] = new IncrementingMonoid[T, T] {
    def zero = z
    def plus(l: T, r: T) = p(l, r)
    def inc(t: T, e: T) = p(t, e)
  }
}

import com.redhat.et.silex.maps.prefixsum.infra._

case class PrefixSumMap[K, V, P](root: RBNodePS[K, V, P]) extends PrefixSumMapLike[K, V, P, PrefixSumMap[K, V, P]] {
  def build(n: RBNode[K, V]) = PrefixSumMap(n.asInstanceOf[RBNodePS[K, V, P]])

  override def toString =
    "PrefixSumMap(" + iterator.zip(prefixSumsIterator()).map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") + ")"
}

object PrefixSumMap {
  class Reify[K, V, P](val keyOrdering: Ordering[K], val prefixMonoid: IncrementingMonoid[P, V]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) = new Reify[K, V, P](keyOrdering, prefixMonoid) with RNodePS[K, V, P] {
      val key = k
      val value = v
      val lsub = ls.asInstanceOf[RBNodePS[K, V, P]]
      val rsub = rs.asInstanceOf[RBNodePS[K, V, P]]
      val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
    }
    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) = new Reify[K, V, P](keyOrdering, prefixMonoid) with BNodePS[K, V, P] {
      val key = k
      val value = v
      val lsub = ls.asInstanceOf[RBNodePS[K, V, P]]
      val rsub = rs.asInstanceOf[RBNodePS[K, V, P]]
      val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
    }
  }

  trait Get
  def key[K](implicit ord: Ordering[K]) = new Get {
    def value[V] = new Get {
      def prefix[P](implicit mon: IncrementingMonoid[P, V]) =
        PrefixSumMap(new Reify[K, V, P](ord, mon) with LeafPS[K, V, P])
    }
  }
}
