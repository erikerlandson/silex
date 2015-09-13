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
  final def +(kv: (K, V)) = RBNode.blacken(ins(kv._1, kv._2))
  final def insert(k: K, v: V) = RBNode.blacken(ins(k, v))
  final def delete(k: K) = RBNode.blacken(del(k))

  def get(k: K): Option[V]

  final def increment(k: K, v: V) = inc(k, v)

  final def prefixSum(k: K, open: Boolean = false) = pfSum(k, pim.zero, open)
  final def prefixSumVal(k: K, open: Boolean = false) = pfSV(k, pim.zero, open)

  final def keyOrdering = ord
  final def valueSemigroup = vsg
  final def prefixMonoid = pim

  // internal
  def pfSum(k: K, sum: P, open: Boolean): P
  def pfSV(k: K, sum: P, open: Boolean): (P, Option[V])
  def ins(k: K, v: V): RBNode[K, V, P]
  def inc(k: K, v: V): RBNode[K, V, P]
  def del(k: K): RBNode[K, V, P]
  def ppv: P
}

case class Leaf[K, V, P]()(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends RBNode[K, V, P] {
  def get(k: K) = None

  def pfSum(k: K, sum: P, open: Boolean) = sum
  def pfSV(k: K, sum: P, open: Boolean) = (sum, None)
  def ins(k: K, v: V) = RNode(k, v, pim.zero, this, this)
  def inc(k: K, v: V) = RNode(k, v, pim.zero, this, this)
  def del(k: K) = this
  def ppv = pim.zero
}

sealed abstract class INode[K, V, P](implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends RBNode[K, V, P]

case class RNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends INode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def pfSum(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSum(k, sum, open) else if (ord.gt(k, key)) rnode.pfSum(k, pim.plus(sum, ppv), open) else if (open) pim.plus(sum, prefix) else pim.plus(sum, ppv)
  def pfSV(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSV(k, sum, open) else if (ord.gt(k, key)) rnode.pfSV(k, pim.plus(sum, ppv), open) else if (open) (pim.plus(sum, prefix), Some(value)) else (pim.plus(sum, ppv), Some(value))

  def ins(k: K, v: V) =
    if (ord.lt(k, key)) {
      val lnew = lnode.ins(k, v)
      RNode(key, value, lnew.ppv, lnew, rnode)
    }
    else if (ord.gt(k, key)) RNode(key, value, prefix, lnode, rnode.ins(k, v))
    else RNode(key, v, prefix, lnode, rnode)

  def inc(k: K, v: V) =
    if (ord.lt(k, key)) {
      val lnew = lnode.inc(k, v)
      RNode(key, value, lnew.ppv, lnew, rnode)
    }
    else if (ord.gt(k, key)) RNode(key, value, prefix, lnode, rnode.inc(k, v))
    else RNode(key, vsg.plus(value, v), prefix, lnode, rnode)

  def del(k: K) =
    if (ord.lt(k, key)) {
      val lnew = lnode.del(k)
      RNode(key, value, lnew.ppv, lnew, rnode)
    }
    else if (ord.gt(k, key)) RNode(key, value, prefix, lnode, rnode.del(k))
    else RBNode.append(lnode, rnode)

  def ppv = pim.inc(prefix, value)
}

case class BNode[K, V, P](key: K, value: V, prefix: P, lnode: RBNode[K, V, P], rnode: RBNode[K, V, P])
    (implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) extends INode[K, V, P] {
  def get(k: K) =
    if (ord.lt(k, key)) lnode.get(k) else if (ord.gt(k, key)) rnode.get(k) else Some(value)

  def pfSum(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSum(k, sum, open) else if (ord.gt(k, key)) rnode.pfSum(k, pim.plus(sum, ppv), open) else if (open) pim.plus(sum, prefix) else pim.plus(sum, ppv)
  def pfSV(k: K, sum: P, open: Boolean) =
    if (ord.lt(k, key)) lnode.pfSV(k, sum, open) else if (ord.gt(k, key)) rnode.pfSV(k, pim.plus(sum, ppv), open) else if (open) (pim.plus(sum, prefix), Some(value)) else (pim.plus(sum, ppv), Some(value))

  def ins(k: K, v: V) =
    if (ord.lt(k, key)) {
      val lnew = lnode.ins(k, v)
      RBNode.balance(BNode(key, value, lnew.ppv, lnew, rnode))
    }
    else if (ord.gt(k, key)) RBNode.balance(BNode(key, value, prefix, lnode, rnode.ins(k, v)))
    else BNode(key, v, prefix, lnode, rnode)

  def inc(k: K, v: V) =
    if (ord.lt(k, key)) {
      val lnew = lnode.inc(k, v)
      RBNode.balance(BNode(key, value, lnew.ppv, lnew, rnode))
    }
    else if (ord.gt(k, key)) RBNode.balance(BNode(key, value, prefix, lnode, rnode.inc(k, v)))
    else BNode(key, vsg.plus(value, v), prefix, lnode, rnode)

  def del(k: K) =
    if (ord.lt(k, key)) RBNode.balanceLeft(key, value, lnode.del(k), rnode)
    else if (ord.gt(k, key)) RBNode.balanceRight(key, value, lnode, rnode.del(k))
    else RBNode.append(lnode, rnode)

  def ppv = pim.inc(prefix, value)
}

object RBNode {
  import scala.language.implicitConversions
  implicit def fromPrefixTreeMap[K, V, P](rbm: PrefixTreeMap[K, V, P]): RBNode[K, V, P] = rbm.node

  def blacken[K, V, P](node: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) = node match {
    case RNode(k, v, p, l, r) => BNode(k, v, p, l, r)
    case n => n
  }
  def redden[K, V, P](node: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) = node match {
    case BNode(k, v, p, l, r) => RNode(k, v, p, l, r)
    case n => n
  }

  def balanceDel[K, V, P](x: K, xv: V, tl: RBNode[K, V, P], tr: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]): RBNode[K, V, P] = (tl, tr) match {
    case (RNode(y, yv, yp, a, b), RNode(z, zv, zp, c, d)) => RNode(x, xv, pim.inc(yp, yv), BNode(y, yv, yp, a, b), BNode(z, zv, zp, c, d))
    case (RNode(y, yv, yp, RNode(z, zv, zp, a, b), c), d) => RNode(y, yv, pim.inc(zp, zv), BNode(z, zv, zp, a, b), BNode(x, xv, c.ppv, c, d))
    case (RNode(y, yv, yp, a, RNode(z, zv, zp, b, c)), d) => RNode(z, zv, pim.inc(yp, yv), BNode(y, yv, yp, a, b), BNode(x, xv, c.ppv, c, d))
    case (a, RNode(y, yv, yp, b, RNode(z, zv, zp, c, d))) => RNode(y, yv, pim.inc(a.ppv, xv), BNode(x, xv, a.ppv, a, b), BNode(z, zv, zp, c, d))
    case (a, RNode(y, yv, yp, RNode(z, zv, zp, b, c), d)) => RNode(z, zv, pim.inc(a.ppv, xv), BNode(x, xv, a.ppv, a, b), BNode(y, yv, c.ppv, c, d))
    case (a, b) => BNode(x, xv, a.ppv, a, b)
  }

  def balanceLeft[K, V, P](x: K, xv: V, tl: RBNode[K, V, P], tr: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]): RBNode[K, V, P] = (tl, tr) match {
    case (RNode(y, yv, yp, a, b), c) => RNode(x, xv, pim.inc(yp, yv), BNode(y, yv, yp, a, b), c)
    case (bl, BNode(y, yv, yp, a, b)) => balanceDel(x, xv, bl, RNode(y, yv, yp, a, b))
    case (bl, RNode(y, yv, yp, BNode(z, zv, zp, a, b), c)) => RNode(z, zv, pim.inc(bl.ppv, xv), BNode(x, xv, bl.ppv, bl, a), balanceDel(y, yv, b, redden(c)))
    case _ => Leaf()
  }

  def balanceRight[K, V, P](x: K, xv: V, tl: RBNode[K, V, P], tr: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]): RBNode[K, V, P] = (tl, tr) match {
    case (a, RNode(y, yv, yp, b, c)) => RNode(x, xv, a.ppv, a, BNode(y, yv, yp, b, c))
    case (BNode(y, yv, yp, a, b), bl) => balanceDel(x, xv, RNode(y, yv, yp, a, b), bl)
    case (RNode(y, yv, yp, a, BNode(z, zv, zp, b, c)), bl) => {
      val lnew = balanceDel(y, yv, redden(a), b)
      RNode(z, zv, lnew.ppv, lnew, BNode(x, xv, c.ppv, c, bl))
    }
    case _ => Leaf()
  }

  def append[K, V, P](tl: RBNode[K, V, P], tr: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]): RBNode[K, V, P] = (tl, tr) match {
    case (Leaf(), n) => n
    case (n, Leaf()) => n
    case (RNode(x, xv, xp, a, b), RNode(y, yv, yp, c, d)) => append(b, c) match {
      case RNode(z, zv, zp, bb, cc) => RNode(z, zv, pim.inc(xp, xv), RNode(x, xv, xp, a, bb), RNode(y, yv, cc.ppv, cc, d))
      case bc => RNode(x, xv, xp, a, RNode(y, yv, bc.ppv, bc, d))
    }
    case (BNode(x, xv, xp, a, b), BNode(y, yv, yp, c, d)) => append(b, c) match {
      case RNode(z, zv, zp, bb, cc) => RNode(z, zv, pim.inc(xp, xv), BNode(x, xv, xp, a, bb), BNode(y, yv, cc.ppv, cc, d))
      case bc => balanceLeft(x, xv, a, BNode(y, yv, bc.ppv, bc, d))
    }
    case (a, RNode(x, xv, xp, b, c)) => {
      val lnew = append(a, b)
      RNode(x, xv, lnew.ppv, lnew, c)
    }
    case (RNode(x, xv, xp, a, b), c) => RNode(x, xv, xp, a, append(b, c))
    case _ => Leaf()
  }

  def balance[K, V, P](node: RBNode[K, V, P])(implicit ord: Ordering[K], vsg: Semigroup[V], pim: IncrementingMonoid[P, V]) = node match {
    case BNode(kG, vG, pG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP), rG) => RNode(kP, vP, pim.inc(pC, vC), BNode(kC, vC, pC, lC, rC), BNode(kG, vG, rP.ppv, rP, rG))
    case BNode(kG, vG, pG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC)), rG) => RNode(kC, vC, pim.inc(pP, vP), BNode(kP, vP, pP, lP, lC), BNode(kG, vG, rC.ppv, rC, rG))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, RNode(kC, vC, pC, lC, rC), rP)) => RNode(kC, vC, pim.inc(pG, vG), BNode(kG, vG, pG, lG, lC), BNode(kP, vP, rC.ppv, rC, rP))
    case BNode(kG, vG, pG, lG, RNode(kP, vP, pP, lP, RNode(kC, vC, pC, lC, rC))) => RNode(kP, vP, pim.inc(pG, vG), BNode(kG, vG, pG, lG, lP), BNode(kC, vC, pC, lC, rC))
    case _ => node
  }

  def apply[K, V, P](vsg: Semigroup[V], pim: IncrementingMonoid[P, V])(implicit ord: Ordering[K]): RBNode[K, V, P] =
    Leaf[K, V, P]()(ord, vsg, pim)

/*
  def apply[K, V, P](kv: (K, V)*)(implicit ord: Ordering[K]) =
    new RBMap(kv.foldLeft(Leaf[K, V]() :RBNode[K, V])((m, e) => m + e))
*/
}

class INodeIterator[K, V, P](c: INode[K, V, P], l: RBNode[K, V, P], r: RBNode[K, V, P]) extends Iterator[INode[K, V, P]] {
  // At any point in time, only one iterator is stored, which is important because
  // otherwise we'd instantiate all sub-iterators over the entire tree.  This way iterators
  // get GC'd once they are spent, and only a linear stack is instantiated at any one time.
  var state = INodeIterator.stateL
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
    while (!n.hasNext && state < INodeIterator.stateR) n = itrState
    n
  }

  // Get the iterator corresponding to next iteration state
  def itrState = {
    val i = state match {
      case INodeIterator.stateL => INodeIterator(l)    // left subtree
      case INodeIterator.stateC => Iterator.single(c)   // current node
      case INodeIterator.stateR => INodeIterator(r)    // right subtree
      case _ => Iterator.empty
    }
    state += 1
    i
  }
}

private object INodeIterator {
  // Iteration states corresponding to in-order tree traversal 
  val stateL = 1  // iterating over left subtree
  val stateC = 2  // current node
  val stateR = 3  // iterating over right subtree

  // Given a node, create an iterator over it and its sub-trees
  def apply[K, V, P](node: RBNode[K, V, P]) = node match {
    case Leaf() => Iterator.empty
    case n: RNode[K, V, P] => new INodeIterator(n, n.lnode, n.rnode)
    case n: BNode[K, V, P] => new INodeIterator(n, n.lnode, n.rnode)
  }
}

class PrefixTreeMap[K, V, P](val node: RBNode[K, V, P]) extends AnyVal {
  def +(kv: (K, V)) = new PrefixTreeMap(node + kv)
  def insert(k: K, v: V) = new PrefixTreeMap(node.insert(k, v))

  def -(k: K) = new PrefixTreeMap(node.delete(k))
  def delete(k: K) = new PrefixTreeMap(node.delete(k))

  def increment(k: K, v: V) = new PrefixTreeMap(node.increment(k, v))

  def get(k: K) = node.get(k)

  def prefixSum(k: K, open: Boolean = false) = node.prefixSum(k, open)
  def prefixSumVal(k: K, open: Boolean = false) = node.prefixSumVal(k, open)

  def iterator = nodesIterator.map {
    case n: RNode[K, V, P] => ((n.key, n.value))
    case n: BNode[K, V, P] => ((n.key, n.value))
  }

  def nodes = nodesIterator.toIterable
  def nodesIterator = INodeIterator(node)

  def keys = keysIterator.toIterable
  def keysIterator = nodesIterator.map {
    case n: RNode[K, V, P] => n.key
    case n: BNode[K, V, P] => n.key
  }

  def values = valuesIterator.toIterable
  def valuesIterator = nodesIterator.map {
    case n: RNode[K, V, P] => n.value
    case n: BNode[K, V, P] => n.value
  }

  def prefixSums(open: Boolean = false) = prefixSumsIterator(open).toIterable
  def prefixSumsIterator(open: Boolean = false) = keysIterator.map(node.prefixSum(_, open))

  def keyOrdering = node.keyOrdering
  def valueSemigroup = node.valueSemigroup
  def prefixMonoid = node.prefixMonoid

  override def toString = node.toString
}

object PrefixTreeMap {
  def apply[K, V, P](vsg: Semigroup[V], pim: IncrementingMonoid[P, V])(implicit ord: Ordering[K]) = new PrefixTreeMap(Leaf[K, V, P]()(ord, vsg, pim))
}

/*
    // https://lampsvn.epfl.ch/trac/scala/browser/scala/tags/R_2_8_1_final/src//library/scala/collection/immutable/RedBlack.scala#L1
    // Based on Stefan Kahrs' Haskell version of Okasaki's Red&Black Trees
84	    // http://www.cse.unsw.edu.au/~dons/data/RedBlackTree.html
85	    def del(k: A): Tree[B] = {
86	      def balance(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
87	        case (RedTree(y, yv, a, b), RedTree(z, zv, c, d)) => RedTree(x, xv, BlackTree(y, yv, a, b), BlackTree(z, zv, c, d))
89	        case (RedTree(y, yv, RedTree(z, zv, a, b), c), d) => RedTree(y, yv, BlackTree(z, zv, a, b), BlackTree(x, xv, c, d))
91	        case (RedTree(y, yv, a, RedTree(z, zv, b, c)), d) => RedTree(z, zv, BlackTree(y, yv, a, b), BlackTree(x, xv, c, d))
93	        case (a, RedTree(y, yv, b, RedTree(z, zv, c, d))) => RedTree(y, yv, BlackTree(x, xv, a, b), BlackTree(z, zv, c, d))
95	        case (a, RedTree(y, yv, RedTree(z, zv, b, c), d)) => RedTree(z, zv, BlackTree(x, xv, a, b), BlackTree(y, yv, c, d))
97	        case (a, b) => BlackTree(x, xv, a, b)
99	      }
100	      def subl(t: Tree[B]) = t match {
101	        case BlackTree(x, xv, a, b) => RedTree(x, xv, a, b)
102	        case _ => error("Defect: invariance violation; expected black, got "+t)
103	      }
104	      def balLeft(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
105	        case (RedTree(y, yv, a, b), c) => RedTree(x, xv, BlackTree(y, yv, a, b), c)
107	        case (bl, BlackTree(y, yv, a, b)) => balance(x, xv, bl, RedTree(y, yv, a, b))
109	        case (bl, RedTree(y, yv, BlackTree(z, zv, a, b), c)) => RedTree(z, zv, BlackTree(x, xv, bl, a), balance(y, yv, b, subl(c)))
111	        case _ => error("Defect: invariance violation at "+right)
112	      }
113	      def balRight(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
114	        case (a, RedTree(y, yv, b, c)) =>  RedTree(x, xv, a, BlackTree(y, yv, b, c))
116	        case (BlackTree(y, yv, a, b), bl) => balance(x, xv, RedTree(y, yv, a, b), bl)
118	        case (RedTree(y, yv, a, BlackTree(z, zv, b, c)), bl) => RedTree(z, zv, balance(y, yv, subl(a), b), BlackTree(x, xv, c, bl))
120	        case _ => error("Defect: invariance violation at "+left)
121	      }
122	      def delLeft = left match {
123	        case _: BlackTree[_] => balLeft(key, value, left.del(k), right)
124	        case _ => RedTree(key, value, left.del(k), right)
125	      }
126	      def delRight = right match {
127	        case _: BlackTree[_] => balRight(key, value, left, right.del(k))
128	        case _ => RedTree(key, value, left, right.del(k))
129	      }
130           def append(tl: Tree[B], tr: Tree[B]): Tree[B] = (tl, tr) match {
131	        case (Empty, t) => t
132	        case (t, Empty) => t
133	        case (RedTree(x, xv, a, b), RedTree(y, yv, c, d)) =>
134	          append(b, c) match {
135	            case RedTree(z, zv, bb, cc) => RedTree(z, zv, RedTree(x, xv, a, bb), RedTree(y, yv, cc, d))
136	            case bc => RedTree(x, xv, a, RedTree(y, yv, bc, d))
137	          }
138	        case (BlackTree(x, xv, a, b), BlackTree(y, yv, c, d)) =>
139	          append(b, c) match {
140	            case RedTree(z, zv, bb, cc) => RedTree(z, zv, BlackTree(x, xv, a, bb), BlackTree(y, yv, cc, d))
141	            case bc => balLeft(x, xv, a, BlackTree(y, yv, bc, d))
142	          }
143	        case (a, RedTree(x, xv, b, c)) => RedTree(x, xv, append(a, b), c)
144	        case (RedTree(x, xv, a, b), c) => RedTree(x, xv, a, append(b, c))
145	      }
146	      // RedBlack is neither A : Ordering[A], nor A <% Ordered[A]
147	      k match {
148	        case _ if isSmaller(k, key) => delLeft
149	        case _ if isSmaller(key, k) => delRight
150	        case _ => append(left, right)
151	      }
152	    }
153	
*/

/*
+      def balance(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
+        case (RedTree(y, yv, a, b), RedTree(z, zv, c, d)) =>
+          RedTree(x, xv, BlackTree(y, yv, a, b), BlackTree(z, zv, c, d))
+        case (RedTree(y, yv, RedTree(z, zv, a, b), c), d) =>
+          RedTree(y, yv, BlackTree(z, zv, a, b), BlackTree(x, xv, c, d))
+        case (RedTree(y, yv, a, RedTree(z, zv, b, c)), d) =>
+          RedTree(z, zv, BlackTree(y, yv, a, b), BlackTree(x, xv, c, d))
+        case (a, RedTree(y, yv, b, RedTree(z, zv, c, d))) =>
+          RedTree(y, yv, BlackTree(x, xv, a, b), BlackTree(z, zv, c, d))
+        case (a, RedTree(y, yv, RedTree(z, zv, b, c), d)) =>
+          RedTree(z, zv, BlackTree(x, xv, a, b), BlackTree(y, yv, c, d))
+        case (a, b) => 
+          BlackTree(x, xv, a, b)
       }
+      def subl(t: Tree[B]) = t match {
+        case BlackTree(x, xv, a, b) => RedTree(x, xv, a, b)
+        case _ => error("Defect: invariance violation; expected black, got "+t)
+      }
+      def balLeft(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
+        case (RedTree(y, yv, a, b), c) => 
+          RedTree(x, xv, BlackTree(y, yv, a, b), c)
+        case (bl, BlackTree(y, yv, a, b)) => 
+          balance(x, xv, bl, RedTree(y, yv, a, b))
+        case (bl, RedTree(y, yv, BlackTree(z, zv, a, b), c)) => 
+          RedTree(z, zv, BlackTree(x, xv, bl, a), balance(y, yv, b, subl(c)))
+        case _ => error("Defect: invariance violation at "+right)
+      }
+      def balRight(x: A, xv: B, tl: Tree[B], tr: Tree[B]) = (tl, tr) match {
+        case (a, RedTree(y, yv, b, c)) =>
+          RedTree(x, xv, a, BlackTree(y, yv, b, c))
+        case (BlackTree(y, yv, a, b), bl) =>
+          balance(x, xv, RedTree(y, yv, a, b), bl)
+        case (RedTree(y, yv, a, BlackTree(z, zv, b, c)), bl) =>
+          RedTree(z, zv, balance(y, yv, subl(a), b), BlackTree(x, xv, c, bl))
+        case _ => error("Defect: invariance violation at "+left)
+      }
+      def delLeft = left match {
+        case _: BlackTree[_] => balLeft(key, value, left.del(k), right)
+        case _ => RedTree(key, value, left.del(k), right)
+      }
+      def delRight = right match {
+        case _: BlackTree[_] => balRight(key, value, left, right.del(k))
+        case _ => RedTree(key, value, left, right.del(k))
+      }
+      def append(tl: Tree[B], tr: Tree[B]): Tree[B] = (tl, tr) match {
+        case (Empty, t) => t
+        case (t, Empty) => t
+        case (RedTree(x, xv, a, b), RedTree(y, yv, c, d)) =>
+          append(b, c) match {
+            case RedTree(z, zv, bb, cc) => RedTree(z, zv, RedTree(x, xv, a, bb), RedTree(y, yv, cc, d))
+            case bc => RedTree(x, xv, a, RedTree(y, yv, bc, d))
+          }
+        case (BlackTree(x, xv, a, b), BlackTree(y, yv, c, d)) =>
+          append(b, c) match {
+            case RedTree(z, zv, bb, cc) => RedTree(z, zv, BlackTree(x, xv, a, bb), BlackTree(y, yv, cc, d))
+            case bc => balLeft(x, xv, a, BlackTree(y, yv, bc, d))
+          }
+        case (a, RedTree(x, xv, b, c)) => RedTree(x, xv, append(a, b), c)
+        case (RedTree(x, xv, a, b), c) => RedTree(x, xv, a, append(b, c))
+      }
*/