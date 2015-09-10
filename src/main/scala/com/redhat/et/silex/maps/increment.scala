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

package com.redhat.et.silex.maps.increment

import math.Ordering

import com.twitter.algebird.Monoid

object tree {
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait of R/B tree nodes supporting increment */
  trait RBNodeInc[K, V] extends RBNode[K, V] {
    /** The monoid that defines what it means to increment a value */
    val valueMonoid: Monoid[V]

    /** Increment the value at a key, by another value */
    final def increment(k: K, iv: V) = blacken(inc(k, iv))

    def inc(k: K, iv: V): RBNode[K, V]
  }

  /** Leaf R/B node supporting increment */
  trait LeafInc[K, V] extends RBNodeInc[K, V] with Leaf[K, V] {
    def inc(k: K, iv: V) = rNode(k, valueMonoid.plus(valueMonoid.zero, iv), this, this)
  }

  /** Internal R/B node supporting increment */
  trait INodeInc[K, V] extends RBNodeInc[K, V] with INode[K, V] {
    val lsub: RBNodeInc[K, V]
    val rsub: RBNodeInc[K, V]
  }

  /** Red R/B node supporting increment */
  trait RNodeInc[K, V] extends INodeInc[K, V] with RNode[K, V] {
    def inc(k: K, iv: V) =
      if (keyOrdering.lt(k, key)) rNode(key, value, lsub.inc(k, iv), rsub)
      else if (keyOrdering.gt(k, key)) rNode(key, value, lsub, rsub.inc(k, iv))
      else rNode(key, valueMonoid.plus(value, iv), lsub, rsub)
  }

  /** Black R/B node supporting increment */
  trait BNodeInc[K, V] extends INodeInc[K, V] with BNode[K, V] {
    def inc(k: K, iv: V) =
      if (keyOrdering.lt(k, key)) balance(bNode(key, value, lsub.inc(k, iv), rsub))
      else if (keyOrdering.gt(k, key)) balance(bNode(key, value, lsub, rsub.inc(k, iv)))
      else bNode(key, valueMonoid.plus(value, iv), lsub, rsub)
  }
}

import com.redhat.et.silex.maps.ordered.tree.RBNode
import tree._

object infra {
  import com.redhat.et.silex.maps.ordered.infra._

  class Inject[K, V](val keyOrdering: Ordering[K], val valueMonoid: Monoid[V]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering, valueMonoid) with RNodeInc[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeInc[K, V]]
        val rsub = rs.asInstanceOf[RBNodeInc[K, V]]
      }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering, valueMonoid) with BNodeInc[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeInc[K, V]]
        val rsub = rs.asInstanceOf[RBNodeInc[K, V]]
      }
  }

  /** An inheritable and mixable trait for adding increment operation to ordered maps 
    * @tparam K The key type
    * @tparam V The value type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait IncrementMapLike[K, V, IN <: INodeInc[K, V], M <: IncrementMapLike[K, V, IN, M]] extends
      OrderedMapLike[K, V, IN, M] {

    val root: RBNodeInc[K, V]

    /** Add (w.r.t. valueMonoid) a given value to the value currently stored at key.
      * @note If key is not present, equivalent to insert(k, valueMonoid.plus(valueMonoid.zero, iv)
      */
    def increment(k: K, iv: V) = build(root.increment(k, iv))

    /** The monoid that defines what it means to increment a value */
    def valueMonoid = root.valueMonoid
  }
}

import infra._

/** An ordered map from keys to values, supporting increment of values w.r.t. a monoid
  * @tparam K The key type
  * @tparam V The value type
  */
case class IncrementMap[K, V](root: RBNodeInc[K, V]) extends
    IncrementMapLike[K, V, INodeInc[K, V], IncrementMap[K, V]] {

  def build(n: RBNode[K, V]) = IncrementMap(n.asInstanceOf[RBNodeInc[K, V]])

  override def toString =
    "IncrementMap(" +
      nodesIterator.map(n => s"${n.key} -> ${n.value}").mkString(", ") +
    ")"
}

object IncrementMap {
  /** Instantiate a new empty IncrementMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.increment._
    *
    * // map strings to integers, using default string ordering and default value monoid
    * val map1 = IncrementMap.key[String].value[Int]
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = IncrementMap.key(ord).value[Int]
    * // A custom value monoid, defines what 'increment' means
    * val mon: Monoid[Int] = ...
    * val map2 = IncrementMap.key[String].value(mon)
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V](implicit mon: Monoid[V]) =
      IncrementMap(new Inject[K, V](ord, mon) with LeafInc[K, V])
  }
}
