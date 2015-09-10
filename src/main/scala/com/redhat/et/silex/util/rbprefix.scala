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

sealed abstract class RBNode[K, V](implicit ord: Ordering[K]) {
  final def +(kv: (K, V)) = ins(kv._1, kv._2) match {
    case RNode(k, v, l, r) => BNode(k, v, l, r)
    case n => n
  }

  def get(k: K): Option[V]

  private[rbprefix] def ins(k: K, v: V): RBNode[K, V]
}

case class Leaf[K, V]()(implicit ord: Ordering[K]) extends RBNode[K, V] {
  def get(k: K) = None

  private[rbprefix] def ins(k: K, v: V) = RNode(k, v, this, this)
}

case class RNode[K, V](key: K, value: V, lnode: RBNode[K, V], rnode: RBNode[K, V])
    (implicit ord: Ordering[K]) extends RBNode[K, V] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RNode(key, value, lnode.ins(k, v), rnode)
    else if (ord.gt(k, key)) RNode(key, value, lnode, rnode.ins(k, v))
    else RNode(key, v, lnode, rnode)
}

case class BNode[K, V](key: K, value: V, lnode: RBNode[K, V], rnode: RBNode[K, V])
    (implicit ord: Ordering[K]) extends RBNode[K, V] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  private[rbprefix] def ins(k: K, v: V) =
    if (ord.lt(k, key)) RBNode.balance(BNode(key, value, lnode.ins(k, v), rnode))
    else if (ord.gt(k, key)) RBNode.balance(BNode(key, value, lnode, rnode.ins(k, v)))
    else BNode(key, v, lnode, rnode)
}

private object RBNode {
  import scala.language.implicitConversions
  implicit def fromRBMap[K, V](rbm: RBMap[K, V]): RBNode[K, V] = rbm.node

  def balance[K, V](node: RBNode[K, V])(implicit ord: Ordering[K]) = node match {
    case BNode(kG, vG, RNode(kP, vP, RNode(kC, vC, lC, rC), rP), rG) =>
      RNode(kP, vP, BNode(kC, vC, lC, rC), BNode(kG, vG, rP, rG))
    case BNode(kG, vG, RNode(kP, vP, lP, RNode(kC, vC, lC, rC)), rG) =>
      RNode(kC, vC, BNode(kP, vP, lP, lC), BNode(kG, vG, rC, rG))
    case BNode(kG, vG, lG, RNode(kP, vP, RNode(kC, vC, lC, rC), rP)) =>
      RNode(kC, vC, BNode(kG, vG, lG, lC), BNode(kP, vP, rC, rP))
    case BNode(kG, vG, lG, RNode(kP, vP, lP, RNode(kC, vC, lC, rC))) =>
      RNode(kP, vP, BNode(kG, vG, lG, lP), BNode(kC, vC, lC, rC))
    case _ => node
  }
}

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
