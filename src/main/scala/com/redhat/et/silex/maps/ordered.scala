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

package com.redhat.et.silex.maps.ordered

import math.Ordering

object tree {
  /** Base class of Red Black tree node
    * @tparam K The key type
    * @tparam V The value type
    */
  trait RBNode[K, V] {

    /** The ordering that is applied to key values */
    val keyOrdering: Ordering[K]

    /** Insert the key/value pair (k, v) into the tree */
    final def insert(k: K, v: V) = blacken(ins(k, v))

    /** Delete the key from the tree */
    final def delete(k: K) = if (node(k).isDefined) blacken(del(k)) else this

    /** Obtain the node stored at a given key if it exists, None otherwise */
    def node(k: K): Option[INode[K, V]]

    // internal
    def ins(k: K, v: V): RBNode[K, V]
    def del(k: K): RBNode[K, V]

    /** create a new Red node from a key, value, left subtree and right subtree */
    protected
    def rNode(key: K, value: V, lsub: RBNode[K, V], rsub: RBNode[K, V]): RNode[K, V]

    /** create a new Black node from a key, value, left subtree and right subtree */
    protected
    def bNode(key: K, value: V, lsub: RBNode[K, V], rsub: RBNode[K, V]): BNode[K, V]

    final protected def blacken(node: RBNode[K, V]) = node match {
      case RNode(k, v, l, r) => bNode(k, v, l, r)
      case n => n
    }
    final protected def redden(node: RBNode[K, V]) = node match {
      case BNode(k, v, l, r) => rNode(k, v, l, r)
      case n: RNode[K, V] => n
      case _ => throw new Exception("illegal attempt to make a leaf node red")
    }

    // balance for insertion
    final protected def balance(node: RBNode[K, V]) = node match {
      case BNode(kG, vG, RNode(kP, vP, RNode(kC, vC, lC, rC), rP), rG) =>
        rNode(kP, vP, bNode(kC, vC, lC, rC), bNode(kG, vG, rP, rG))
      case BNode(kG, vG, RNode(kP, vP, lP, RNode(kC, vC, lC, rC)), rG) =>
        rNode(kC, vC, bNode(kP, vP, lP, lC), bNode(kG, vG, rC, rG))
      case BNode(kG, vG, lG, RNode(kP, vP, RNode(kC, vC, lC, rC), rP)) =>
        rNode(kC, vC, bNode(kG, vG, lG, lC), bNode(kP, vP, rC, rP))
      case BNode(kG, vG, lG, RNode(kP, vP, lP, RNode(kC, vC, lC, rC))) =>
        rNode(kP, vP, bNode(kG, vG, lG, lP), bNode(kC, vC, lC, rC))
      case _ => node
    }

    final protected
    def balanceDel(x: K, xv: V, tl: RBNode[K, V], tr: RBNode[K, V]): RBNode[K, V] =
      (tl, tr) match {
      case (RNode(y, yv, a, b), RNode(z, zv, c, d)) =>
        rNode(x, xv, bNode(y, yv, a, b), bNode(z, zv, c, d))
      case (RNode(y, yv, RNode(z, zv, a, b), c), d) =>
        rNode(y, yv, bNode(z, zv, a, b), bNode(x, xv, c, d))
      case (RNode(y, yv, a, RNode(z, zv, b, c)), d) =>
        rNode(z, zv, bNode(y, yv, a, b), bNode(x, xv, c, d))
      case (a, RNode(y, yv, b, RNode(z, zv, c, d))) =>
        rNode(y, yv, bNode(x, xv, a, b), bNode(z, zv, c, d))
      case (a, RNode(y, yv, RNode(z, zv, b, c), d)) =>
        rNode(z, zv, bNode(x, xv, a, b), bNode(y, yv, c, d))
      case (a, b) => bNode(x, xv, a, b)
    }

    final protected
    def balanceLeft(x: K, xv: V, tl: RBNode[K, V], tr: RBNode[K, V]): RBNode[K, V] =
      (tl, tr) match {
        case (RNode(y, yv, a, b), c) => rNode(x, xv, bNode(y, yv, a, b), c)
        case (bl, BNode(y, yv, a, b)) => balanceDel(x, xv, bl, rNode(y, yv, a, b))
        case (bl, RNode(y, yv, BNode(z, zv, a, b), c)) =>
          rNode(z, zv, bNode(x, xv, bl, a), balanceDel(y, yv, b, redden(c)))
        case _ => throw new Exception(s"undefined pattern in tree pair: ($tl, $tr)")
      }

    final protected
    def balanceRight(x: K, xv: V, tl: RBNode[K, V], tr: RBNode[K, V]): RBNode[K, V] =
      (tl, tr) match {
        case (a, RNode(y, yv, b, c)) => rNode(x, xv, a, bNode(y, yv, b, c))
        case (BNode(y, yv, a, b), bl) => balanceDel(x, xv, rNode(y, yv, a, b), bl)
        case (RNode(y, yv, a, BNode(z, zv, b, c)), bl) =>
          rNode(z, zv, balanceDel(y, yv, redden(a), b), bNode(x, xv, c, bl))
        case _ => throw new Exception(s"undefined pattern in tree pair: ($tl, $tr)")
      }

    final protected
    def append(tl: RBNode[K, V], tr: RBNode[K, V]): RBNode[K, V] = (tl, tr) match {
      case (Leaf(), n) => n
      case (n, Leaf()) => n
      case (RNode(x, xv, a, b), RNode(y, yv, c, d)) => append(b, c) match {
        case RNode(z, zv, bb, cc) => rNode(z, zv, rNode(x, xv, a, bb), rNode(y, yv, cc, d))
        case bc => rNode(x, xv, a, rNode(y, yv, bc, d))
      }
      case (BNode(x, xv, a, b), BNode(y, yv, c, d)) => append(b, c) match {
        case RNode(z, zv, bb, cc) => rNode(z, zv, bNode(x, xv, a, bb), bNode(y, yv, cc, d))
        case bc => balanceLeft(x, xv, a, bNode(y, yv, bc, d))
      }
      case (a, RNode(x, xv, b, c)) => rNode(x, xv, append(a, b), c)
      case (RNode(x, xv, a, b), c) => rNode(x, xv, a, append(b, c))
    }

    // NOTE: the balancing rules for node deletion all assume that the case of deleting a key
    // that is not in the map is addressed elsewhere.  If these balancing functions are applied
    // to a key that isn't present, they will fail destructively and uninformatively.
    final protected def delLeft(node: INode[K, V], k: K): RBNode[K, V] = node.lsub match {
      case n: BNode[K, V] => balanceLeft(node.key, node.value, node.lsub.del(k), node.rsub)
      case _ => rNode(node.key, node.value, node.lsub.del(k), node.rsub)
    }

    final protected def delRight(node: INode[K, V], k: K): RBNode[K, V] = node.rsub match {
      case n: BNode[K, V] => balanceRight(node.key, node.value, node.lsub, node.rsub.del(k))
      case _ => rNode(node.key, node.value, node.lsub, node.rsub.del(k))
    }
  }

  /** Represents a leaf node in the Red Black tree system */
  trait Leaf[K, V] extends RBNode[K, V] {
    def node(k: K) = None

    def ins(k: K, v: V) = rNode(k, v, this, this)
    def del(k: K) = this
  }

  private object Leaf {
    def unapply[K, V](node: Leaf[K, V]): Boolean = true
  }

  /** Represents an internal node (Red or Black) in the Red Black tree system */
  trait INode[K, V] extends RBNode[K, V] {
    /** The key this node is stored at */
    val key: K
    /** The value stored with the key */
    val value: V
    /** The left sub-tree */
    val lsub: RBNode[K, V]
    /** The right sub-tree */
    val rsub: RBNode[K, V]

    def node(k: K) =
      if (keyOrdering.lt(k, key)) lsub.node(k)
      else if (keyOrdering.gt(k, key)) rsub.node(k)
      else Some(this)

    def del(k: K) =
      if (keyOrdering.lt(k, key)) delLeft(this, k)
      else if (keyOrdering.gt(k, key)) delRight(this, k)
      else append(lsub, rsub)

    override def toString = s"INode($key,$value)"
  }

  /** Represents a Red node in the Red Black tree system */
  trait RNode[K, V] extends INode[K, V] {
    def ins(k: K, v: V) =
      if (keyOrdering.lt(k, key)) rNode(key, value, lsub.ins(k, v), rsub)
      else if (keyOrdering.gt(k, key)) rNode(key, value, lsub, rsub.ins(k, v))
      else rNode(key, v, lsub, rsub)
  }

  private object RNode {
    def unapply[K, V](node: RNode[K, V]): Option[(K, V, RBNode[K, V], RBNode[K, V])] =
      Some((node.key, node.value, node.lsub, node.rsub))
  }

  /** Represents a Black node in the Red Black tree system */
  trait BNode[K, V] extends INode[K, V] {
    def ins(k: K, v: V) =
      if (keyOrdering.lt(k, key)) balance(bNode(key, value, lsub.ins(k, v), rsub))
      else if (keyOrdering.gt(k, key)) balance(bNode(key, value, lsub, rsub.ins(k, v)))
      else bNode(key, v, lsub, rsub)
  }

  private object BNode {
    def unapply[K, V](node: BNode[K, V]): Option[(K, V, RBNode[K, V], RBNode[K, V])] =
      Some((node.key, node.value, node.lsub, node.rsub))
  }
}

import tree._

object infra {
  class Inject[K, V](val keyOrdering: Ordering[K]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering) with RNode[K, V] {
        val key = k
        val value = v
        val lsub = ls
        val rsub = rs
      }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering) with BNode[K, V] {
        val key = k
        val value = v
        val lsub = ls
        val rsub = rs
      }
  }

  /** Iterator over internal nodes in a Red Black tree, performing in-order traversal */
  class INodeIterator[K, V, IN <: INode[K, V]](node: IN) extends Iterator[IN] {
    // At any point in time, only one iterator is stored, which is important because
    // otherwise we'd instantiate all sub-iterators over the entire tree.  This way iterators
    // get GC'd once they are spent, and only a linear stack is instantiated at any one time.
    private var state = INodeIterator.stateL
    private var itr = itrNext

    def hasNext = itr.hasNext

    def next = {
      val v = itr.next
      if (!itr.hasNext) itr = itrNext
      v
    }

    // Get the next non-empty iterator if it exists, or an empty iterator otherwise
    // Adhere to in-order state transition: left-subtree -> current -> right-subtree 
    private def itrNext = {
      var n = itrState
      while (!n.hasNext && state < INodeIterator.stateR) n = itrState
      n
    }

    // Get the iterator corresponding to next iteration state
    private def itrState = {
      val i = state match {
        case INodeIterator.stateL => INodeIterator.apply[K, V, IN](node.lsub)  // left subtree
        case INodeIterator.stateC => Iterator.single(node)                     // current node
        case INodeIterator.stateR => INodeIterator.apply[K, V, IN](node.rsub)  // right subtree
        case _ => Iterator.empty
      }
      state += 1
      i
    }
  }

  /** Factory and constants for INodeIterator */
  object INodeIterator {
    // Iteration states corresponding to in-order tree traversal 
    private [infra] val stateL = 1  // iterating over left subtree
    private [infra] val stateC = 2  // current node
    private [infra] val stateR = 3  // iterating over right subtree

    def apply[K, V, IN <: INode[K, V]](node: RBNode[K, V]) = node match {
      case n: Leaf[K, V] => Iterator.empty
      case _ => new INodeIterator[K, V, IN](node.asInstanceOf[IN])
    }
  }

  /** An inheritable (and mixable) trait representing Ordered Map functionality that is 
    * backed by a Red/Black tree implemenation.
    * @tparam K The key type
    * @tparam V The value type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait OrderedMapLike[K, V, IN <: INode[K, V], M <: OrderedMapLike[K, V, IN, M]] {
    /** The root of the underlying R/B tree */
    val root: RBNode[K, V]

    /** Build a new ordered map around a Red Black node */
    protected def build(n: RBNode[K, V]): M

    /** Obtain a new map with (key, val) pair inserted */
    def +(kv: (K, V)) = build(root.insert(kv._1, kv._2))

    /** Obtain a new map with a (key, val) pair inserted */
    def insert(k: K, v: V) = build(root.insert(k, v))

    /** Obtain a new map with key removed */
    def -(k: K) = build(root.delete(k))

    /** Obtain a new map with key removed */
    def delete(k: K) = build(root.delete(k))

    /** Get the internal node stored at at key, or None if key is not present */
    def node(k: K) = root.node(k).map(_.asInstanceOf[IN])

    /** Get the value stored at a key, or None if key is not present */
    def get(k: K) = root.node(k).map(_.value)

    /** Returns true if key is present in the map, false otherwise */
    def contains(k: K) = root.node(k).isDefined

    /** Iterator over (key,val) pairs, in key order */
    def iterator = nodesIterator.map(n => ((n.key, n.value)))

    /** A container of underlying nodes, in key order */
    def nodes = nodesIterator.toIterable

    /** Iterator over nodes, in key order */
    def nodesIterator = INodeIterator.apply[K, V, IN](root)

    /** A container of keys, in key order */
    def keys = keysIterator.toIterable

    /** Iterator over keys, in key order */
    def keysIterator = nodesIterator.map(_.key)

    /** Container of values, in key order */
    def values = valuesIterator.toIterable

    /** Iterator over values, in key order */
    def valuesIterator = nodesIterator.map(_.value)

    /** Obtain the Ordering[K] object in use by this ordered map */
    def keyOrdering = root.keyOrdering
  }
}

import infra._

/** A map from keys to values, ordered by key
  * @tparam K The key type
  * @tparam V The value type
  */
case class OrderedMap[K, V](root: RBNode[K, V]) extends
    OrderedMapLike[K, V, INode[K, V], OrderedMap[K, V]] {

  def build(n: RBNode[K, V]) = OrderedMap(n)

  override def toString =
    "OrderedMap(" +
      nodesIterator.map(n => s"${n.key} -> ${n.value}").mkString(", ") +
    ")"
}

object OrderedMap {
  /** Instantiate a new empty OrderedMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.ordered._
    *
    * // map strings to integers, using default string ordering
    * val map1 = OrderedMap.key[String].value[Int]
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = OrderedMap.key(ord).value[Int]
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = new AnyRef {
    def value[V] = OrderedMap(new Inject[K, V](ord) with Leaf[K, V])
  }
}
