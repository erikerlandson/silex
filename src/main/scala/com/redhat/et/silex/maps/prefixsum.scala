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

import com.twitter.algebird.{Monoid, MonoidAggregator}

object tree {
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait for R/B nodes supporting prefix-sum query */
  trait RBNodePS[K, V, P] extends RBNode[K, V] {
    /** Monoid with increment-by-element that defines semantics of prefix sum */
    val prefixMonoid: IncrementingMonoid[P, V]

    /** return the prefix sum of keys <= key, or alternatively strictly < key */
    final def prefixSum(k: K, open: Boolean = false) = pfSum(k, prefixMonoid.zero, open)

    def pfSum(k: K, sum: P, open: Boolean): P
    def pfs: P
  }

  /** Leaf node for R/B nodes supporting prefix-sum query */
  trait LeafPS[K, V, P] extends RBNodePS[K, V, P] with Leaf[K, V] {
    def pfSum(k: K, sum: P, open: Boolean) = sum
    def pfs = prefixMonoid.zero  
  }

  /** Internal node for R/B nodes supporting prefix-sum query */
  trait INodePS[K, V, P] extends RBNodePS[K, V, P] with INode[K, V] {
    val lsub: RBNodePS[K, V, P]
    val rsub: RBNodePS[K, V, P]

    val prefix: P

    def pfSum(k: K, sum: P, open: Boolean) =
      if (keyOrdering.lt(k, key))
        lsub.pfSum(k, sum, open)
      else if (keyOrdering.gt(k, key))
        rsub.pfSum(k, prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), value), open)
      else if (open)
        prefixMonoid.plus(sum, lsub.pfs)
      else
        prefixMonoid.inc(prefixMonoid.plus(sum, lsub.pfs), value)

    def pfs = prefix

    override def toString = s"INodePS($key,$value,$prefix)"
  }

  /** Red node for R/B nodes supporting prefix-sum query */
  trait RNodePS[K, V, P] extends INodePS[K, V, P] with RNode[K, V]

  /** Black node for R/B nodes supporting prefix-sum query */
  trait BNodePS[K, V, P] extends INodePS[K, V, P] with BNode[K, V]
}

import com.redhat.et.silex.maps.ordered.tree.RBNode
import tree._

object infra {
  import com.redhat.et.silex.maps.ordered.infra._

  class Inject[K, V, P](val keyOrdering: Ordering[K], val prefixMonoid: IncrementingMonoid[P, V]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V, P](keyOrdering, prefixMonoid) with RNodePS[K, V, P] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodePS[K, V, P]]
        val rsub = rs.asInstanceOf[RBNodePS[K, V, P]]
        val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
     }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V, P](keyOrdering, prefixMonoid) with BNodePS[K, V, P] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodePS[K, V, P]]
        val rsub = rs.asInstanceOf[RBNodePS[K, V, P]]
        val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
     }
  }

  /** An inheritable and mixable trait for adding prefix sum query to ordered maps 
    * @tparam K The key type
    * @tparam V The value type
    * @tparam P The prefix sum type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait PrefixSumMapLike[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M]]
      extends OrderedMapLike[K, V, IN, M] {

    val root: RBNodePS[K, V, P]

    /** Obtain the prefix (cumulative) sum of values <= a given key 'k'.
      * If 'open' is true, sums the open interval for keys strictly < k.
      * If 'k' is not present in the map, then the sum for keys < k is returned.
      */
    def prefixSum(k: K, open: Boolean = false) = root.prefixSum(k, open)

    /** A container of all prefix sums over the stored values.  If 'open' is true,
      * the sums will be for strictly < each key.
      */
    def prefixSums(open: Boolean = false) = prefixSumsIterator(open).toIterable

    /** Iterate over prefix sums for stored values.  If 'open' is true,
      * the sums will be for strictly < each key.
      */
    def prefixSumsIterator(open: Boolean = false) = {
      val mon = root.prefixMonoid
      val itr = valuesIterator.scanLeft(mon.zero)((p,e)=>mon.inc(p,e))
      if (open) itr.takeWhile(_ => itr.hasNext) else itr.drop(1)
    }

    /** The incrementing monoid that defines the semantics of prefix sum */
    def prefixMonoid = root.prefixMonoid
  }
}

import infra._

/** A monoid that also supports an 'increment' operation.  This class is intended to encode
  * semantics similar to Scala's seq.aggregate(z)(seqop,combop), where the standard
  * Monoid 'plus' corresponds to 'combop' and the 'inc' method corresponds to 'seqop'.
  */
trait IncrementingMonoid[T, E] extends Monoid[T] {
  /** increment a monoid 't' by an element value 'e', and return the result */
  def inc(t: T, e: E): T
}

/** Factory methods for instantiating incrementing monoids */
object IncrementingMonoid {
  /** Create an incrementing monoid from a MonoidAggregator object.
    * 'zero' and 'plus' are inherited from agg.monoid.
    * 'inc' is defined by: agg.monoid.plus(t,agg.prepare(e))
    */
  def fromMonoidAggregator[T, E](agg: MonoidAggregator[E, T, T]) = new IncrementingMonoid[T, E] {
    def zero = agg.monoid.zero
    def plus(l: T, r: T) = agg.monoid.plus(l, r)
    def inc(t: T, e: E) = agg.monoid.plus(t, agg.prepare(e))
  }

  /** Create an incrementing monoid from a Monoid object.
    * 'zero' and 'plus' are inherited from monoid.
    * 'inc' is equivalent to 'plus'
    */
  def fromMonoid[T](implicit monoid: Monoid[T]) = new IncrementingMonoid[T, T] {
    def zero = monoid.zero
    def plus(l: T, r: T) = monoid.plus(l, r)
    def inc(t: T, e: T) = monoid.plus(t, e)
  }

  /** Create an incrementing monoid from zero, plus and inc 
    * {{{
    * import scala.language.reflectiveCalls
    * IncrementingMonoid.zero(0).plus(_ + _).inc(_ + _)
    * IncrementingMonoid.zero(Set.empty[Int].plus(_ ++ _).inc[Int](_ + _)
    * }}}
    */
  def zero[T](z: T) = new AnyRef {
    def plus(p: (T, T) => T) = new AnyRef {
      def inc[E](i: (T, E) => T) = new IncrementingMonoid[T, E] {
        def zero = z
        def plus(l: T, r: T) = p(l, r)
        def inc(t: T, e: E) = i(t, e)
      } 
    }
  }
}

/** An ordered map that supports a prefix-sum query
  * @tparam K The key type
  * @tparam V The value type
  */
case class PrefixSumMap[K, V, P](root: RBNodePS[K, V, P]) extends
    PrefixSumMapLike[K, V, P, INodePS[K, V, P], PrefixSumMap[K, V, P]] {

  def build(n: RBNode[K, V]) = PrefixSumMap(n.asInstanceOf[RBNodePS[K, V, P]])

  override def toString =
    "PrefixSumMap(" +
      iterator.zip(prefixSumsIterator())
        .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
    ")"
}

object PrefixSumMap {
  /** Instantiate a new empty PrefixSumMap from key, value and prefix types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.prefixsum._
    *
    * // map strings to integers, using default ordering and standard integer monoid
    * val map1 = PrefixSumMap.key[String].value[Int].prefix(IncrementingMonoid.fromMonoid[Int])
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = PrefixSumMap.key(ord).value[Int].prefix(IncrementingMonoid.fromMonoid[Int])
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V] = new AnyRef {
      def prefix[P](implicit mon: IncrementingMonoid[P, V]) =
        PrefixSumMap(new Inject[K, V, P](ord, mon) with LeafPS[K, V, P])
    }
  }
}
