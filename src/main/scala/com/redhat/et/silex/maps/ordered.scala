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
  import com.redhat.et.silex.maps.redblack.tree._

  trait DataMap[K, V] extends Data[K] {
    val value: V
  }

  /** Base class of ordered K/V tree node
    * @tparam K The key type
    * @tparam V The value type
    */
  trait NodeMap[K, V] extends Node[K]

  trait LNodeMap[K, V] extends NodeMap[K, V] with LNode[K]

  trait INodeMap[K, V] extends NodeMap[K, V] with INode[K] {
    val data: DataMap[K, V]
  }
}

import com.redhat.et.silex.maps.redblack.tree.{ Node, INode, LNode }
import tree._

object infra {
  import com.redhat.et.silex.maps.redblack.infra.INodeIterator
  import com.redhat.et.silex.maps.redblack.tree.{ Color, Data, Node, INode }

  /** An inheritable (and mixable) trait representing Ordered container functionality that is 
    * backed by a Red/Black tree implemenation.
    * @tparam K The key type
    * @tparam IN The internal node type of the underlying R/B tree subclass
    * @tparam M The container self-type of the concrete map subclass
    */
  trait OrderedLike[K, IN <: INode[K], M <: OrderedLike[K, IN, M]] {
    /** The root of the underlying R/B tree */
    val root: Node[K]

    /** Build a new ordered container around a Red Black node */
    protected def build(n: Node[K]): M

    /** Obtain a new container with data inserted */
    def insertData(dat: Data[K]) = build(root.insert(dat))

    /** Obtain a new container with key removed */
    def delete(k: K) = build(root.delete(k))

    /** Obtain a new map with key removed */
    def -(k: K) = build(root.delete(k))

    /** Get the internal node stored at at key, or None if key is not present */
    def node(k: K) = root.node(k).map(_.asInstanceOf[IN])

    /** Returns true if key is present in the container, false otherwise */
    def contains(k: K) = root.node(k).isDefined

    /** A container of underlying nodes, in key order */
    def nodes = nodesIterator.toIterable

    /** Iterator over nodes, in key order */
    def nodesIterator = INodeIterator.apply[K, IN](root)

    /** A container of keys, in key order */
    def keys = keysIterator.toIterable

    /** Iterator over keys, in key order */
    def keysIterator = nodesIterator.map(_.data.key)

    /** Obtain the Ordering[K] object in use by this ordered map */
    def keyOrdering = root.keyOrdering
  }

  /** An inheritable (and mixable) trait representing Ordered Set functionality that is 
    * backed by a Red/Black tree implemenation.
    * @tparam K The key type
    * @tparam IN The internal node type of the underlying R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait OrderedSetLike[K, IN <: INode[K], M <: OrderedSetLike[K, IN, M]]
      extends OrderedLike[K, IN, M] {
    /** The root of the underlying R/B tree */
    val root: Node[K]

    /** Obtain a new container with key inserted */
    def +(k: K) = this.insert(k)

    /** Obtain a new container with key inserted */
    def insert(k: K) = build(root.insert(
      new Data[K] {
        val key = k
      }))

    /** Iterator over keys, in key order */
    def iterator = nodesIterator.map(_.data.key)
  }

  /** An inheritable (and mixable) trait representing Ordered Map functionality that is 
    * backed by a Red/Black tree implemenation.
    * @tparam K The key type
    * @tparam V The value type
    * @tparam IN The internal node type of the underlying R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait OrderedMapLike[K, V, IN <: INodeMap[K, V], M <: OrderedMapLike[K, V, IN, M]]
      extends OrderedLike[K, IN, M] {
    /** The root of the underlying R/B tree */
    val root: NodeMap[K, V]

    /** Obtain a new map with (key, val) pair inserted */
    def +(kv: (K, V)) = this.insert(kv._1, kv._2)

    /** Obtain a new map with a (key, val) pair inserted */
    def insert(k: K, v: V) = build(root.insert(
      new DataMap[K, V] {
        val key = k
        val value = v
      }))

    /** Get the value stored at a key, or None if key is not present */
    def get(k: K) = this.node(k).map(_.data.value)

    /** Iterator over (key,val) pairs, in key order */
    def iterator = nodesIterator.map(n => ((n.data.key, n.data.value)))

    /** Container of values, in key order */
    def values = valuesIterator.toIterable

    /** Iterator over values, in key order */
    def valuesIterator = nodesIterator.map(_.data.value)
  }

  class InjectSet[K](val keyOrdering: Ordering[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectSet[K](keyOrdering) with INode[K] {
        // INode[K]
        val color = clr
        val lsub = ls
        val rsub = rs
        val data = dat
      }
  }

  class InjectMap[K, V](val keyOrdering: Ordering[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectMap[K, V](keyOrdering) with INodeMap[K, V] {
        // INode[K]
        val color = clr
        val lsub = ls
        val rsub = rs
        val data = dat.asInstanceOf[DataMap[K, V]]
      }
  }
}

import infra._

/** An ordered set of keys
  * @tparam K The key type
  */
case class OrderedSet[K](root: Node[K]) extends
    OrderedSetLike[K, INode[K], OrderedSet[K]] {

  def build(n: Node[K]) = OrderedSet(n)

  override def toString =
    "OrderedSet(" +
      nodesIterator.map(n => s"${n.data.key}").mkString(", ") +
    ")"
}

object OrderedSet {
  /** Instantiate a new empty OrderedSet from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.ordered._
    *
    * // map strings to integers, using default string ordering
    * val set1 = OrderedSet.key[String]
    * // Use a custom ordering
    * val ord: Ordering[String] = ...
    * val map2 = OrderedSet.key(ord)
    * }}}
    */
  def key[K](implicit ord: Ordering[K]) = OrderedSet(new InjectSet[K](ord) with LNode[K])
}

/** A map from keys to values, ordered by key
  * @tparam K The key type
  * @tparam V The value type
  */
case class OrderedMap[K, V](root: NodeMap[K, V]) extends
    OrderedMapLike[K, V, INodeMap[K, V], OrderedMap[K, V]] {

  def build(n: Node[K]) = OrderedMap(n.asInstanceOf[NodeMap[K, V]])

  override def toString =
    "OrderedMap(" +
      nodesIterator.map(n => s"${n.data.key} -> ${n.data.value}").mkString(", ") +
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
    def value[V] = OrderedMap(new InjectMap[K, V](ord) with LNodeMap[K, V])
  }
}
