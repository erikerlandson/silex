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

package com.redhat.et.silex.util.rbprefix

import math.Ordering

import com.twitter.algebird.{ Semigroup, Monoid, MonoidAggregator }

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

sealed abstract class RBNode[K, V, P](implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) {
  final def +(kv: (K, V)) = ins(kv._1, kv._2) match {
    case RNode(k, v, p, l, r) => BNode(k, v, p, l, r)
    case n => n
  }

  def get(k: K): Option[V]
  final def prefixSum(k: K, open: Boolean = false) = pfSum(k, pim.zero, open)
  final def prefixSumVal(k: K, open: Boolean = false) = pfSV(k, pim.zero, open)

  // internal
  def pfSum(k: K, sum: P, open: Boolean): P
  def pfSV(k: K, sum: P, open: Boolean): (P, Option[V])
  private[rbprefix] def ins(k: K, v: V): RBNode[K, V, P]
  def ppv: P
}

case class Leaf[K, V, P]()(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends RBNode[K, V, P] {
  def get(k: K) = None

  def pfSum(k: K, sum: P, open: Boolean) = sum
  def pfSV(k: K, sum: P, open: Boolean) = (sum, None)
  private[rbprefix] def ins(k: K, v: V) = RNode(k, v, pim.zero, this, this)
  def ppv = pim.zero
}

case class RNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends RBNode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def pfSum(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSum(k, sum, open) else if (ord.gt(k, key)) rnode.pfSum(k, pim.plus(sum, ppv), open) else if (open) pim.plus(sum, prefix) else pim.plus(sum, ppv)
  def pfSV(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSV(k, sum, open) else if (ord.gt(k, key)) rnode.pfSV(k, pim.plus(sum, ppv), open) else if (open) (pim.plus(sum, prefix), Some(value)) else (pim.plus(sum, ppv), Some(value))

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RNode(key, value, pim.inc(prefix, v), lnode.ins(k, v), rnode)
    else if (ord.gt(k, key)) RNode(key, value, prefix, lnode, rnode.ins(k, v))
    else RNode(key, v, prefix, lnode, rnode)

  def ppv = pim.inc(prefix, value)
}

case class BNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends RBNode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def pfSum(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSum(k, sum, open) else if (ord.gt(k, key)) rnode.pfSum(k, pim.plus(sum, ppv), open) else if (open) pim.plus(sum, prefix) else pim.plus(sum, ppv)
  def pfSV(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSV(k, sum, open) else if (ord.gt(k, key)) rnode.pfSV(k, pim.plus(sum, ppv), open) else if (open) (pim.plus(sum, prefix), Some(value)) else (pim.plus(sum, ppv), Some(value))

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RBNode.balance(BNode(key, value, pim.inc(prefix, v), lnode.ins(k, v), rnode))
    else if (ord.gt(k, key)) RBNode.balance(BNode(key, value, prefix, lnode, rnode.ins(k, v)))
    else BNode(key, v, prefix, lnode, rnode)

  def ppv = pim.inc(prefix, value)
}

object RBNode {
  import scala.language.implicitConversions
  //implicit def fromRBMap[K, V](rbm: RBMap[K, V]): RBNode[K, V] = rbm.node

  def balance[K, V, P](node: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) = node match {
    case BNode(kG, vG, pG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP), rG) => RNode(kP, vP, pim.inc(pC, vC), BNode(kC, vC, pC, lC, rC), BNode(kG, vG, rP.ppv, rP, rG))
    case BNode(kG, vG, pG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC)), rG) => RNode(kC, vC, pim.inc(pP, vP), BNode(kP, vP, pP, lP, lC), BNode(kG, vG, rC.ppv, rC, rG))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP)) => RNode(kC, vC, pim.inc(pG, vG), BNode(kG, vG, pG, lG, lC), BNode(kP, vP, rC.ppv, rC, rP))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC))) => RNode(kP, vP, pim.inc(pG, vG), BNode(kG, vG, pG, lG, lP), BNode(kC, vC, pC, lC, rC))
    case _ => node
  }

  def empty[K, V](implicit ord: Ordering[K], mon: Monoid[V]): RBNode[K, V, V] = Leaf[K, V, V]()(ord, mon, IncrementingMonoid.fromMonoid(mon))

/*
  def apply[K, V, P](kv: (K, V)*)(implicit ord: Ordering[K]) =
    new RBMap(kv.foldLeft(Leaf[K, V]() :RBNode[K, V])((m, e) => m + e))
*/
}

/*
class RBNodeIterator[K, V](kv: (K, V), l: RBNode[K, V], r: RBNode[K, V]) extends Iterator[(K, V)] {
  // At any point in time, only one iterator is stored, which is important because
  // otherwise we'd instantiate all sub-iterators over the entire tree.  This way iterators
  // get GC'd once they are spent, and only a linear stack is instantiated at any one time.
  var state = RBNodeIterator.stateL
  var itr = itrNext

  def hasNext = itr.hasNext

  def next = {
    val v = itr.next
    if (!itr.hasNext) itr = itrNext
    v
  }

  // Get the next non-empty iterator if it exists, or an empty iterator otherwise
  // Adhere to in-order state transition: left-subtree -> current -> right-subtree 
  def itrNext = {
    var n = itrState
    while (!n.hasNext && state < RBNodeIterator.stateR) n = itrState
    n
  }

  // Get the iterator corresponding to next iteration state
  def itrState = {
    val i = state match {
      case RBNodeIterator.stateL => RBNodeIterator(l)    // left subtree
      case RBNodeIterator.stateC => Iterator.single(kv)  // current node
      case RBNodeIterator.stateR => RBNodeIterator(r)    // right subtree
      case _ => Iterator.empty
    }
    state += 1
    i
  }
}

private object RBNodeIterator {
  // Iteration states corresponding to in-order tree traversal 
  val stateL = 1  // iterating over left subtree
  val stateC = 2  // current node
  val stateR = 3  // iterating over right subtree

  // Given a node, create an iterator over it and its sub-trees
  def apply[K, V](node: RBNode[K, V]) = node match {
    case Leaf() => Iterator.empty
    case n: RNode[K, V] => new RBNodeIterator(((n.key, n.value)), n.lnode, n.rnode)
    case n: BNode[K, V] => new RBNodeIterator(((n.key, n.value)), n.lnode, n.rnode)
  }
}

class RBMap[K, V](val node: RBNode[K, V]) extends AnyVal {
  def +(kv: (K, V)) = new RBMap(node + kv)
  def get(k: K) = node.get(k)
  def iterator = RBNodeIterator(node)
  override def toString = node.toString
}

object RBMap {
  def empty[K, V](implicit ord: Ordering[K]) = new RBMap(Leaf[K, V]())

  def apply[K, V](kv: (K, V)*)(implicit ord: Ordering[K]) =
    new RBMap(kv.foldLeft(Leaf[K, V]() :RBNode[K, V])((m, e) => m + e))
}

*/
