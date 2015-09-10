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
  import com.redhat.et.silex.maps.ordered.tree._

  /** Base trait of R/B tree nodes supporting nearest-key query */
  trait RBNodeNear[K, V] extends RBNode[K, V] {
    /** Ordering that also supports linear distance |x-y| */
    val keyOrdering: Numeric[K] // <: Ordering[K]

    /** Obtain the nearest k/v pairs to a given key */
    def nearest(k: K): Seq[(K, V)]

    final def dist(k1: K, k2: K) = keyOrdering.abs(keyOrdering.minus(k1, k2))
  }

  /** Leaf R/B tree nodes supporting nearest-key query */
  trait LeafNear[K, V] extends RBNodeNear[K, V] with Leaf[K, V] {
    def nearest(k: K) = Seq.empty[(K, V)]
  }

  /** Internal R/B tree nodes supporting nearest-key query */
  trait INodeNear[K, V] extends RBNodeNear[K, V] with INode[K, V] {
    val lsub: RBNodeNear[K, V]
    val rsub: RBNodeNear[K, V]

    val kmin: K
    val kmax: K

    def nearest(k: K) = {
      if (keyOrdering.lt(k, key)) {
        lsub match {
          case ls: INodeNear[K, V] => {
            if (keyOrdering.lteq(k, ls.kmax)) ls.nearest(k)
            else {
              val (dk, ldk) = (dist(k, key), dist(k, ls.kmax))
              if (keyOrdering.lt(dk, ldk)) Seq(((key, value)))
              else if (keyOrdering.gt(dk, ldk)) Seq(((ls.kmax, ls.node(ls.kmax).get.value)))
              else Seq(((ls.kmax, ls.node(ls.kmax).get.value)), ((key, value)))
            }
          }
          case _ => Seq(((key, value)))
        }
      } else if (keyOrdering.gt(k, key)) {
        rsub match {
          case rs: INodeNear[K, V] => {
            if (keyOrdering.gteq(k, rs.kmin)) rs.nearest(k)
            else {
              val (dk, rdk) = (dist(k, key), dist(k, rs.kmin))
              if (keyOrdering.lt(dk, rdk)) Seq(((key, value)))
              else if (keyOrdering.gt(dk, rdk)) Seq(((rs.kmin, rs.node(rs.kmin).get.value)))
              else Seq(((key, value)), ((rs.kmin, rs.node(rs.kmin).get.value)))
            }
          }
          case _ => Seq(((key, value)))
        }
      } else Seq(((key, value)))
    }

    override def toString = s"INodeNear($key, $value, $kmin, $kmax)"
  }

  /** Red R/B tree nodes supporting nearest-key query */
  trait RNodeNear[K, V] extends INodeNear[K, V] with RNode[K, V]

  /** Black R/B tree nodes supporting nearest-key query */
  trait BNodeNear[K, V] extends INodeNear[K, V] with BNode[K, V]
}

import com.redhat.et.silex.maps.ordered.tree.RBNode
import tree._

object infra {
  import com.redhat.et.silex.maps.ordered.infra._

  class Inject[K, V](val keyOrdering: Numeric[K]) {
    def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering) with RNodeNear[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeNear[K, V]]
        val rsub = rs.asInstanceOf[RBNodeNear[K, V]]
        val kmin = lsub match {
          case n: INodeNear[K, V] => n.kmin
          case _ => key
        }
        val kmax = rsub match {
          case n: INodeNear[K, V] => n.kmax
          case _ => key
        }
      }

    def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
      new Inject[K, V](keyOrdering) with BNodeNear[K, V] {
        val key = k
        val value = v
        val lsub = ls.asInstanceOf[RBNodeNear[K, V]]
        val rsub = rs.asInstanceOf[RBNodeNear[K, V]]
        val kmin = lsub match {
          case n: INodeNear[K, V] => n.kmin
          case _ => key
        }
        val kmax = rsub match {
          case n: INodeNear[K, V] => n.kmax
          case _ => key
        }
      }
  }

  /** An inheritable and mixable trait for adding nearest-key query to ordered maps 
    * @tparam K The key type
    * @tparam V The value type
    * @tparam IN The node type of the concrete internal R/B tree subclass
    * @tparam M The map self-type of the concrete map subclass
    */
  trait NearestMapLike[K, V, IN <: INodeNear[K, V], M <: NearestMapLike[K, V, IN, M]] extends
      OrderedMapLike[K, V, IN, M] {

    val root: RBNodeNear[K, V]

    /** Return (key,val) pair(s) nearest to a given key.  The sequence of (key,val) returned may
      * have zero, one or two elements.  If (k) is at the midpoint between two keys, the two 
      * nearest will be returned.  If map is empty, an empty sequence will be returned.
      */
    def nearest(k: K) = root.nearest(k)
  }
}

import infra._

/** An ordered map from keys to values, supporting a nearest-key query
  * @tparam K The key type
  * @tparam V The value type
  */
case class NearestMap[K, V](root: RBNodeNear[K, V]) extends
    NearestMapLike[K, V, INodeNear[K, V], NearestMap[K, V]] {

  def build(n: RBNode[K, V]) = NearestMap(n.asInstanceOf[RBNodeNear[K, V]])

  override def toString =
    "NearestMap(" +
      nodesIterator.map(n => s"${n.key} -> ${n.value}").mkString(", ") +
    ")"
}

object NearestMap {
  /** Instantiate a new empty NearestMap from key and value types
    * {{{
    * import scala.language.reflectiveCalls
    * import com.redhat.et.silex.maps.nearest._
    *
    * // map strings to integers, using default Numeric[String]
    * val map1 = NearestMap.key[String].value[Int]
    * // Use a custom numeric
    * val num: Numeric[String] = ...
    * val map2 = NearestMap.key(num).value[Int]
    * }}}
    */
  def key[K](implicit num: Numeric[K]) = new AnyRef {
    def value[V] =
      NearestMap(new Inject[K, V](num) with LeafNear[K, V])
  }
}
