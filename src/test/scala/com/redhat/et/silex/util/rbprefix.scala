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

import org.scalatest._

import com.twitter.algebird.{ Semigroup, Monoid, MonoidAggregator }

import com.redhat.et.silex.testing.matchers._

object RBNodeProperties extends FlatSpec with Matchers {
  trait Color
  case object Red extends Color
  case object Black extends Color

  def color[K, V, P](node: RBNode[K, V, P]): Color = node match {
    case n: RNode[K, V, P] => Red
    case _ => Black
  }

  def blackHeight[K, V, P](node: RBNode[K, V, P]): Set[Int] = node match {
    case Leaf() => Set(1)
    case RNode(_, _, _, lnode, rnode) => blackHeight(lnode) ++ blackHeight(rnode)
    case BNode(_, _, _, lnode, rnode) => (blackHeight(lnode) ++ blackHeight(rnode)).map(_ + 1)
  }

  def testBlackHeight[K, V, P](node: RBNode[K, V, P]): Boolean = node match {
    case Leaf() => true

    case RNode(_, _, _, lnode, rnode) =>
      testBlackHeight(lnode) && testBlackHeight(rnode) &&
      (blackHeight(lnode) == blackHeight(rnode)) && (blackHeight(lnode).size == 1)
      
    case BNode(_, _, _, lnode, rnode) =>
      testBlackHeight(lnode) && testBlackHeight(rnode) &&
      (blackHeight(lnode) == blackHeight(rnode)) && (blackHeight(lnode).size == 1)
  }

  def testRedChildrenBlack[K, V, P](node: RBNode[K, V, P]): Boolean = node match {
    case Leaf() => true
    case n: RNode[K, V, P] => color(n.lnode) == Black && color(n.rnode) == Black &&
      testRedChildrenBlack(n.lnode) && testRedChildrenBlack(n.rnode)
    case n: BNode[K, V, P] => testRedChildrenBlack(n.lnode) && testRedChildrenBlack(n.rnode)
  }

  def balance[K, V, P](node: RBNode[K, V, P]): (Int, Int) = node match {
    case Leaf() => (0, 0)
    case n: RNode[K, V, P] => {
      val (lmin, lmax) = balance(n.lnode)
      val (rmin, rmax) = balance(n.rnode)
      (1 + math.min(lmin, rmin), 1 + math.max(lmax, rmax))
    }
    case n: BNode[K, V, P] => {
      val (lmin, lmax) = balance(n.lnode)
      val (rmin, rmax) = balance(n.rnode)
      (1 + math.min(lmin, rmin), 1 + math.max(lmax, rmax))
    }
  }

  def testBalance[K, V, P](node: RBNode[K, V, P]): Boolean = node match {
    case Leaf() => true
    case n: RNode[K, V, P] => {
      val (pmin, pmax) = balance(node)
      (pmax <= (2 * pmin)) && testBalance(n.lnode) && testBalance(n.rnode)
    }
    case n: BNode[K, V, P] => {
      val (pmin, pmax) = balance(node)
      (pmax <= (2 * pmin)) && testBalance(n.lnode) && testBalance(n.rnode)
    }
  }

  // test RB tree invariant properties related to RB construction
  def testRB[K, V, P](root: RBNode[K, V, P]) = {
    // The root node of a RB tree should be black
    color(root) should be (Black)

    // Every path from a node to its descendant leafs should contain the same # of black nodes
    testBlackHeight(root) should be (true)

    // If a node is red, then both its children should be black
    testRedChildrenBlack(root) should be (true)

    // Depth of deepest node should be <= twice the depth of shallowest
    testBalance(root) should be (true)
  }
}


class PrefixTreeMapSpec extends FlatSpec with Matchers {
  import RBNodeProperties._

  // Int keys, values and prefixes, using standard integer addition
  def mapType1 = PrefixTreeMap[Int, Int, Int](implicitly[Semigroup[Int]],IncrementingMonoid.fromMonoid(implicitly[Monoid[Int]]))

  // Assumes 'data' is in key order
  def testKV[K, V, P](data: Seq[(K, V)], ptmap: PrefixTreeMap[K, V, P]) {
    // verify the map elements are ordered by key
    ptmap.keys should beEqSeq(data.map(_._1))

    // verify the map correctly preserves key -> value mappings
    data.map(x => ptmap.get(x._1)) should beEqSeq(data.map(x => Option(x._2)))
    ptmap.values should beEqSeq(data.map(_._2))
  }

  // Assumes 'data' is in key order
  def testPrefix[K, V, P](data: Seq[(K, V)], ptmap: PrefixTreeMap[K, V, P]) {
    val mon = ptmap.prefixMonoid
    val psTruth = data.map(_._2).scanLeft(mon.zero)((v, e) => mon.inc(v, e))
    ptmap.prefixSums() should beEqSeq(psTruth.tail)
    ptmap.prefixSums(open=true) should beEqSeq(psTruth.dropRight(1))
    ptmap.prefixSums() should beEqSeq(ptmap.keys.map(k => ptmap.prefixSum(k)))
    ptmap.prefixSums(open=true) should beEqSeq(ptmap.keys.map(k => ptmap.prefixSum(k, open=true)))
  }

  // Assumes 'data' is in key order
  def testDel[K, V, P](data: Seq[(K, V)], ptmap: PrefixTreeMap[K, V, P]) {
    data.iterator.map(_._1).foreach { key =>
      val delMap = ptmap.delete(key)
      val delData = data.filter(_._1 != key)
      testRB(delMap)
      testKV(delData, delMap)
      testPrefix(delData, delMap)
    }
  }

  it should "pass exhaustive tree patterns" in {
    // N should remain small, because we are about to exhaustively test N! patterns.
    // Values larger than 8 rapidly start to take a lot of time, for example my runs for
    // N = 10 complete in about 20 minutes.
    val N = 8
    (0 to N).foreach { n =>
      val data = Vector.tabulate(n)(j => (j, j))
      data.permutations.foreach { shuffle =>
        val ptmap = shuffle.foldLeft(mapType1)((m, e) => m + e)
        testRB(ptmap)
        testKV(data, ptmap)
        testDel(data, ptmap)
        testPrefix(data, ptmap)
      }
    }
  }

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(100)(j => (j, j))
    (1 to 100).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val ptmap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(ptmap)
      testKV(data, ptmap)
      testDel(data, ptmap)
      testPrefix(data, ptmap)
    }
  }
}
