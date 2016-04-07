/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2016 Red Hat, Inc.
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
 * limitations under the License.
 */

package com.redhat.et.silex.cluster

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.mutable

import org.apache.spark.mllib.tree.model.{ Node, DecisionTreeModel }
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.linalg.{Vector => SparkVector}

object ClusteringNode {
  def apply(node: Node): ClusteringNode = {
    node match {
      case n: ClusteringNode => n
      case _ => new ClusteringNode(node)
    }
  }

  case class Predicate(feature: String, op: Predicate.Op, threshold: Double) {
    override def toString = {
      val opstr = op match {
        case Predicate.LE => "<="
        case Predicate.GT => ">"
      }
      "(" + feature + " " + opstr + " " + threshold.toString + ")"
    }
  }

  object Predicate {
    sealed trait Op
    case object LE extends Op
    case object GT extends Op
  }
}

// Construction is recursive on leftNode and rightNode, with a basis case where they are None:
class ClusteringNode(node: Node) 
    extends Node(node.id,
                 node.predict,
                 node.impurity,
                 node.isLeaf,
                 node.split,
                 node.leftNode.map(ClusteringNode(_)),
                 node.rightNode.map(ClusteringNode(_)),
                 node.stats) {

  import ClusteringNode.Predicate

  def flatten: Seq[ClusteringNode] = {
    if (isLeaf) List(this)
    else List(this) ++
      leftNode.get.asInstanceOf[ClusteringNode].flatten ++
      rightNode.get.asInstanceOf[ClusteringNode].flatten
  }

  private def rulesImpl(
      names: PartialFunction[Int, String],
      pstack: List[Predicate],
      rmap: mutable.Map[Double, mutable.ArrayBuffer[Seq[Predicate]]]) {
    if (isLeaf) {
      val cat = this.predict.predict
      if (!rmap.contains(cat)) rmap += (cat -> mutable.ArrayBuffer.empty[Seq[Predicate]])
      rmap(cat) += pstack.reverse
    } else {
      val t = this.split.get.threshold
      val f = this.split.get.feature
      val fname = names.applyOrElse(f, (x:Int) => "f" + x.toString)
      leftNode.get.asInstanceOf[ClusteringNode]
        .rulesImpl(names, Predicate(fname, Predicate.LE, t)::pstack, rmap)
      rightNode.get.asInstanceOf[ClusteringNode]
        .rulesImpl(names, Predicate(fname, Predicate.GT, t)::pstack, rmap)
    }
  }

  def rules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] = {
    val rmap = mutable.Map.empty[Double, mutable.ArrayBuffer[Seq[Predicate]]]
    rulesImpl(names, List.empty[Predicate], rmap)
    rmap.toMap
  }

  def predictLeafId(features: SparkVector): Int = {
    if (isLeaf) {
      id
    } else {
      val ln = leftNode.get.asInstanceOf[ClusteringNode]
      val rn = rightNode.get.asInstanceOf[ClusteringNode]
      if (split.get.featureType == Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          ln.predictLeafId(features)
        } else {
          rn.predictLeafId(features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          ln.predictLeafId(features)
        } else {
          rn.predictLeafId(features)
        }
      }
    }
  }
}

class ClusteringTreeModel(root: Node, algo: Algo)
    extends DecisionTreeModel(ClusteringNode(root), algo) {
  import ClusteringNode.Predicate

  def predictLeafId(features: SparkVector): Int = {
    this.topNode.asInstanceOf[ClusteringNode].predictLeafId(features)
  }

  def flatten: Seq[Node] = this.topNode.asInstanceOf[ClusteringNode].flatten

  def rules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] =
    this.topNode.asInstanceOf[ClusteringNode].rules(names)
}
