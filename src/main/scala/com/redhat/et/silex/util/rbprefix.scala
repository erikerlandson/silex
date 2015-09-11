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

sealed abstract class RBNode[K, V, P](implicit zero: P, rollupOp: (P, V) => P, prefixOp: (P, P) => P, ord: Ordering[K]) {
  final def +(kv: (K, V)) = ins(kv._1, kv._2) match {
    case RNode(k, v, p, l, r) => BNode(k, v, p, l, r)
    case n => n
  }

  def get(k: K): Option[V]
  final def getPrefix(k: K) = getPre(k, zero)

  // internal
  def getPre(k: K, preSum: P): P 
  private[rbprefix] def ins(k: K, v: V): RBNode[K, V, P]
  def ppv: P
}

case class Leaf[K, V, P]()(implicit zero: P, rollupOp: (P, V) => P, prefixOp: (P, P) => P, ord: Ordering[K]) extends RBNode[K, V, P] {
  def get(k: K) = None

  def getPre(k: K, preSum: P) = preSum
  private[rbprefix] def ins(k: K, v: V) = RNode(k, v, zero, this, this)
  def ppv = zero
}

case class RNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit zero: P, rollupOp: (P, V) => P, prefixOp: (P, P) => P, ord: Ordering[K]) extends RBNode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def getPre(k: K, preSum: P) =
    if (ord.lt(k, key)) lnode.getPre(k, preSum) else if (ord.gt(k, key)) rnode.getPre(k, prefixOp(preSum, rollupOp(prefix, value))) else prefixOp(preSum, prefix)

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RNode(key, value, rollupOp(prefix, v), lnode.ins(k, v), rnode)
    else if (ord.gt(k, key)) RNode(key, value, prefix, lnode, rnode.ins(k, v))
    else RNode(key, v, prefix, lnode, rnode)

  def ppv = rollupOp(prefix, value)
}

case class BNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit zero: P, rollupOp: (P, V) => P, prefixOp: (P, P) => P, ord: Ordering[K]) extends RBNode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def getPre(k: K, preSum: P) =
    if (ord.lt(k, key)) lnode.getPre(k, preSum) else if (ord.gt(k, key)) rnode.getPre(k, prefixOp(preSum, rollupOp(prefix, value))) else prefixOp(preSum, prefix)

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RBNode.balance(BNode(key, value, rollupOp(prefix, v), lnode.ins(k, v), rnode))
    else if (ord.gt(k, key)) RBNode.balance(BNode(key, value, prefix, lnode, rnode.ins(k, v)))
    else BNode(key, v, prefix, lnode, rnode)

  def ppv = rollupOp(prefix, value)
}

object RBNode {
  import scala.language.implicitConversions
  //implicit def fromRBMap[K, V](rbm: RBMap[K, V]): RBNode[K, V] = rbm.node

  def balance[K, V, P](node: RBNode[K, V, P])(implicit zero: P, rollupOp: (P, V) => P, prefixOp: (P, P) => P, ord: Ordering[K]) = node match {
    case BNode(kG, vG, pG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP), rG) => RNode(kP, vP, rollupOp(pC, vC), BNode(kC, vC, pC, lC, rC), BNode(kG, vG, rP.ppv, rP, rG))
    case BNode(kG, vG, pG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC)), rG) => RNode(kC, vC, rollupOp(pP, vP), BNode(kP, vP, pP, lP, lC), BNode(kG, vG, rC.ppv, rC, rG))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP)) => RNode(kC, vC, rollupOp(pG, vG), BNode(kG, vG, pG, lG, lC), BNode(kP, vP, rC.ppv, rC, rP))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC))) => RNode(kP, vP, rollupOp(pG, vG), BNode(kG, vG, pG, lG, lP), BNode(kC, vC, pC, lC, rC))
    case _ => node
  }

  def empty[K, V, P](z: P)(rollupOp: (P, V) => P, prefixOp: (P, P) => P)(implicit ord: Ordering[K]): RBNode[K, V, P] = Leaf[K, V, P]()(z, rollupOp, prefixOp, ord)

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
