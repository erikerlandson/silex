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

package com.redhat.et.silex.maps.nearest

import math.Numeric

object tree {
  import com.redhat.et.silex.maps.redblack.tree._
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait of R/B tree nodes supporting nearest-key query */
  trait NodeNear[K] extends Node[K] {
    /** Ordering that also supports linear distance |x-y| */
    val keyOrdering: Numeric[K] // <: Ordering[K]

    /** Obtain the nearest nodes to a given key */
    def nearest(k: K): Seq[INodeNear[K]]

    final def dist(k1: K, k2: K) = keyOrdering.abs(keyOrdering.minus(k1, k2))
  }

  /** Leaf R/B tree nodes supporting nearest-key query */
  trait LNodeNear[K] extends NodeNear[K] with LNode[K] {
    def nearest(k: K) = Seq.empty[INodeNear[K]]
  }

  /** Internal R/B tree nodes supporting nearest-key query */
  trait INodeNear[K] extends NodeNear[K] with INode[K] {
    val lsub: NodeNear[K]
    val rsub: NodeNear[K]

    val kmin: K
    val kmax: K

    def nearest(k: K) = {
      if (keyOrdering.lt(k, data.key)) {
        lsub match {
          case ls: INodeNear[K] => {
            if (keyOrdering.lteq(k, ls.kmax)) ls.nearest(k)
            else {
              val (dk, ldk) = (dist(k, data.key), dist(k, ls.kmax))
              if (keyOrdering.lt(dk, ldk)) Seq(this)
              else if (keyOrdering.gt(dk, ldk))
                Seq(ls.node(ls.kmax).get.asInstanceOf[INodeNear[K]])
              else Seq(ls.node(ls.kmax).get.asInstanceOf[INodeNear[K]], this)
            }
          }
          case _ => Seq(this)
        }
      } else if (keyOrdering.gt(k, data.key)) {
        rsub match {
          case rs: INodeNear[K] => {
            if (keyOrdering.gteq(k, rs.kmin)) rs.nearest(k)
            else {
              val (dk, rdk) = (dist(k, data.key), dist(k, rs.kmin))
              if (keyOrdering.lt(dk, rdk)) Seq(this)
              else if (keyOrdering.gt(dk, rdk))
                Seq(rs.node(rs.kmin).get.asInstanceOf[INodeNear[K]])
              else Seq(this, rs.node(rs.kmin).get.asInstanceOf[INodeNear[K]])
            }
          }
          case _ => Seq(this)
        }
      } else Seq(this)
    }

    override def toString = s"INodeNear(${data.key}, $kmin, $kmax)"
  }

  trait NodeNearMap[K, V] extends NodeNear[K] with NodeMap[K, V]
  trait LNodeNearMap[K, V] extends NodeNearMap[K, V] with LNodeNear[K] with LNodeMap[K, V]
  trait INodeNearMap[K, V] extends NodeNearMap[K, V] with INodeNear[K] with INodeMap[K, V] {
    val lsub: NodeNearMap[K, V]
    val rsub: NodeNearMap[K, V]
  }
}

import com.redhat.et.silex.maps.redblack.tree.Node
import tree._

object infra {
  import com.redhat.et.silex.maps.redblack.tree.{ Data, Node, Color }
  import com.redhat.et.silex.maps.ordered.tree.DataMap
  import com.redhat.et.silex.maps.ordered.infra._

  /** An inheritable and mixable trait for adding nearest-key query to ordered containers
    * @tparam K The key type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The self-type of the concrete container
    */
  trait NearestLike[K, IN <: INodeNear[K], M <: NearestLike[K, IN, M]]
      extends OrderedLike[K, IN, M] {

    val root: NodeNear[K]

    def nearestNodes(k: K) = root.nearest(k).map(_.asInstanceOf[IN])
  }

  /** An inheritable and mixable trait for adding nearest-key query to an ordered set
    * @tparam K The key type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The self-type of the concrete container
    */
  trait NearestSetLike[K, IN <: INodeNear[K], M <: NearestSetLike[K, IN, M]]
      extends NearestLike[K, IN, M] with OrderedSetLike[K, IN, M] {

    val root: NodeNear[K]

    /** Return keys nearest to a given key.  The sequence that is returned may
      * have zero, one or two elements.  If (k) is at the midpoint between two keys, the two 
      * nearest will be returned.  If container is empty, an empty sequence will be returned.
      */
    def nearest(k: K) = root.nearest(k).map(_.data.key)
  }

  /** An inheritable and mixable trait for adding nearest-key query to an ordered map
    * @tparam K The key type
    * @tparam V The value type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The self-type of the concrete container
    */
  trait NearestMapLike[K, V, IN <: INodeNearMap[K, V], M <: NearestMapLike[K, V, IN, M]]
      extends NearestLike[K, IN, M] with OrderedMapLike[K, V, IN, M] {

    val root: NodeNearMap[K, V]

    /** Return keys nearest to a given key.  The sequence that is returned may
      * have zero, one or two elements.  If (k) is at the midpoint between two keys, the two 
      * nearest will be returned.  If container is empty, an empty sequence will be returned.
      */
    def nearest(k: K) = root.nearest(k).map { n =>
      val dm = n.data.asInstanceOf[DataMap[K, V]]
      (dm.key, dm.value)
    }
  }

  class InjectSet[K](val keyOrdering: Numeric[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectSet[K](keyOrdering) with INodeNear[K] {
        // INode
        val color = clr
        val lsub = ls.asInstanceOf[NodeNear[K]]
        val rsub = rs.asInstanceOf[NodeNear[K]]
        val data = dat
        // INodeNear
        val kmin = lsub match {
          case n: INodeNear[K] => n.kmin
          case _ => data.key
        }
        val kmax = rsub match {
          case n: INodeNear[K] => n.kmax
          case _ => data.key
        }
      }
  }

  class InjectMap[K, V](val keyOrdering: Numeric[K]) {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectMap[K, V](keyOrdering) with INodeNearMap[K, V] {
        // INode
        val color = clr
        val lsub = ls.asInstanceOf[NodeNearMap[K, V]]
        val rsub = rs.asInstanceOf[NodeNearMap[K, V]]
        val data = dat.asInstanceOf[DataMap[K, V]]
        // INodeNear
        val kmin = lsub match {
          case n: INodeNear[K] => n.kmin
          case _ => data.key
        }
        val kmax = rsub match {
          case n: INodeNear[K] => n.kmax
          case _ => data.key
        }
      }
  }
}

import infra._

/** An ordered set of keys, supporting a nearest-key query
  * @tparam K The key type
  */
case class NearestSet[K](root: NodeNear[K]) extends
    NearestSetLike[K, INodeNear[K], NearestSet[K]] {

  def build(n: Node[K]) = NearestSet(n.asInstanceOf[NodeNear[K]])

  override def toString =
    "NearestSet(" +
      nodesIterator.map(n => s"${n.data.key}").mkString(", ") +
    ")"
}

object NearestSet {
  /** Instantiate a new empty NearestSet 
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.nearest._
    *
    * // set of integers, using default Numeric[Int]
    * val map1 = NearestSet.key[Int]
    * // Use a custom numeric
    * val num: Numeric[Int] = ...
    * val map2 = NearestSet.key(num)
    * }}}
    */
  def key[K](implicit num: Numeric[K]) = NearestSet(new InjectSet[K](num) with LNodeNear[K])
}

/** An ordered map from keys to values, supporting a nearest-key query
  * @tparam K The key type
  * @tparam V The value type
  */
case class NearestMap[K, V](root: NodeNearMap[K, V]) extends
    NearestMapLike[K, V, INodeNearMap[K, V], NearestMap[K, V]] {

  def build(n: Node[K]) = NearestMap(n.asInstanceOf[NodeNearMap[K, V]])

  override def toString =
    "NearestMap(" +
      nodesIterator.map(n => s"${n.data.key} -> ${n.data.value}").mkString(", ") +
    ")"
}

object NearestMap {
  /** Instantiate a new empty NearestMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.nearest._
    *
    * // map integers to strings, using default Numeric[Int]
    * val map1 = NearestMap.key[Int].value[String]
    * // Use a custom numeric
    * val num: Numeric[Int] = ...
    * val map2 = NearestMap.key(num).value[String]
    * }}}
    */
  def key[K](implicit num: Numeric[K]) = new AnyRef {
    def value[V] =
      NearestMap(new InjectMap[K, V](num) with LNodeNearMap[K, V])
  }
}
