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

package com.redhat.et.silex.maps

import org.scalatest._

import com.twitter.algebird.Monoid

import com.redhat.et.silex.testing.matchers._

object mixed {
  import math.Numeric

  import com.redhat.et.silex.maps.ordered.tree._
  import com.redhat.et.silex.maps.ordered.infra._

  import com.redhat.et.silex.maps.increment.tree._
  import com.redhat.et.silex.maps.increment.infra._

  import com.redhat.et.silex.maps.prefixsum.IncrementingMonoid
  import com.redhat.et.silex.maps.prefixsum.tree._
  import com.redhat.et.silex.maps.prefixsum.infra._

  import com.redhat.et.silex.maps.nearest.tree._
  import com.redhat.et.silex.maps.nearest.infra._

  object tree {
    trait RBNodeTD[K, V, P] extends RBNodePS[K, V, P] with RBNodeInc[K, V] with RBNodeNear[K, V]

    trait LeafTD[K, V, P] extends RBNodeTD[K, V, P]
        with LeafPS[K, V, P] with LeafInc[K, V] with LeafNear[K, V]

    trait INodeTD[K, V, P] extends RBNodeTD[K, V, P]
        with INodePS[K, V, P] with INodeInc[K, V] with INodeNear[K, V] {
      val lsub: RBNodeTD[K, V, P]
      val rsub: RBNodeTD[K, V, P]
    }

    trait RNodeTD[K, V, P] extends INodeTD[K, V, P]
        with RNodePS[K, V, P] with RNodeInc[K, V] with RNodeNear[K, V]

    trait BNodeTD[K, V, P] extends INodeTD[K, V, P]
        with BNodePS[K, V, P] with BNodeInc[K, V] with BNodeNear[K, V]
  }

  import tree._

  object infra {
    trait TDigestMapLike[K, V, P, IN <: INodeTD[K, V, P], M <: TDigestMapLike[K, V, P, IN, M]]
        extends PrefixSumMapLike[K, V, P, IN, M]
        with IncrementMapLike[K, V, IN, M]
        with NearestMapLike[K, V, IN, M] {
      val root: RBNodeTD[K, V, P]
    }
  }

  import infra._

  case class TDigestMap[K, V, P](root: RBNodeTD[K, V, P])
      extends TDigestMapLike[K, V, P, INodeTD[K, V, P], TDigestMap[K, V, P]] {

    def build(n: RBNode[K, V]) = TDigestMap(n.asInstanceOf[RBNodeTD[K, V, P]])

    override def toString =
      "TDigestMap(" +
        iterator.zip(prefixSumsIterator())
          .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
      ")"
  }

  object TDigestMap {
    class Reify[K, V, P](
      val keyOrdering: Numeric[K],
      val valueMonoid: Monoid[V],
      val prefixMonoid: IncrementingMonoid[P, V]) {

      def rNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
        new Reify[K, V, P](keyOrdering, valueMonoid, prefixMonoid) with RNodeTD[K, V, P] {
          val key = k
          val value = v
          val lsub = ls.asInstanceOf[RBNodeTD[K, V, P]]
          val rsub = rs.asInstanceOf[RBNodeTD[K, V, P]]
          val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
          val kmin = lsub match {
            case n: INodeTD[K, V, P] => n.kmin
            case _ => key
          }
          val kmax = rsub match {
            case n: INodeTD[K, V, P] => n.kmax
            case _ => key
          }
       }

      def bNode(k: K, v: V, ls: RBNode[K, V], rs: RBNode[K, V]) =
        new Reify[K, V, P](keyOrdering, valueMonoid, prefixMonoid) with BNodeTD[K, V, P] {
          val key = k
          val value = v
          val lsub = ls.asInstanceOf[RBNodeTD[K, V, P]]
          val rsub = rs.asInstanceOf[RBNodeTD[K, V, P]]
          val prefix = prefixMonoid.inc(prefixMonoid.plus(lsub.pfs, rsub.pfs), value)
          val kmin = lsub match {
            case n: INodeTD[K, V, P] => n.kmin
            case _ => key
          }
          val kmax = rsub match {
            case n: INodeTD[K, V, P] => n.kmax
            case _ => key
          }
       }
    }

    def key[K](implicit num: Numeric[K]) = new AnyRef {
      def value[V](implicit vm: Monoid[V]) = new AnyRef {
        def prefix[P](implicit im: IncrementingMonoid[P, V]) =
          TDigestMap(new Reify[K, V, P](num, vm, im) with LeafTD[K, V, P])
      }
    }
  }
}

class MixedMapSpec extends FlatSpec with Matchers {
  import scala.language.reflectiveCalls

  import com.redhat.et.silex.maps.prefixsum.IncrementingMonoid

  import com.redhat.et.silex.maps.ordered.OrderedMapProperties._
  import com.redhat.et.silex.maps.ordered.RBNodeProperties._
  import com.redhat.et.silex.maps.prefixsum.PrefixSumMapProperties._
  import com.redhat.et.silex.maps.increment.IncrementMapProperties._
  import com.redhat.et.silex.maps.nearest.NearestMapProperties._

  import mixed.TDigestMap

  def mapType1 =
    TDigestMap.key[Double].value[Int]
      .prefix(IncrementingMonoid.fromMonoid(implicitly[Monoid[Int]]))

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j.toDouble, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val map = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(map.root)
      testKV(data, map)
      testDel(data, map)
      testPrefix(data, map) 
      testIncrement(data, map) 
      testNearest(data, map) 
    }
  }  
}

