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

class RBMap[K, V](val node: RBNode[K, V]) extends AnyVal {
  def +(kv: (K, V)) = new RBMap(node + kv)
  def get(k: K) = node.get(k)
  override def toString = node.toString
}

object RBMap {
  def empty[K, V](implicit ord: Ordering[K]) = new RBMap(Leaf[K, V]())

  def apply[K, V](kv: (K, V)*)(implicit ord: Ordering[K]) =
    new RBMap(kv.foldLeft(Leaf[K, V]() :RBNode[K, V])((m, e) => m + e))
}
