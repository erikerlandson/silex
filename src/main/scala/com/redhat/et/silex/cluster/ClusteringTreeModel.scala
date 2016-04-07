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
import scala.collection.mutable

import org.apache.spark.mllib.tree.model.{ Node, DecisionTreeModel }
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.linalg.{ Vector => SparkVector }

package infra {
  class ClusteringNode(self: Node) extends Serializable {
    import ClusteringTreeModel._
    import ClusteringNode._

    def nodeIterator: Iterator[Node] = new Iterator[Node] {
      val que = mutable.Queue(self)
      def hasNext = !que.isEmpty
      def next = {
        val nxt = que.dequeue()
        if (!nxt.isLeaf) {
          que.enqueue(nxt.leftNode.get, nxt.rightNode.get)
        }
        nxt
      }
    }

    private [infra] def rulesImpl(
        names: PartialFunction[Int, String],
        pstack: List[Predicate],
        rmap: mutable.Map[Double, mutable.ArrayBuffer[Seq[Predicate]]]) {
      if (self.isLeaf) {
        val cat = self.predict.predict
        if (!rmap.contains(cat)) rmap += (cat -> mutable.ArrayBuffer.empty[Seq[Predicate]])
        rmap(cat) += pstack.reverse
      } else {
        val t = self.split.get.threshold
        val f = self.split.get.feature
        val fname = names.applyOrElse(f, defaultName)
        self.leftNode.get
          .rulesImpl(names, Predicate(fname, Predicate.LE, t)::pstack, rmap)
        self.rightNode.get
          .rulesImpl(names, Predicate(fname, Predicate.GT, t)::pstack, rmap)
      }
    }

    def rules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] = {
      val rmap = mutable.Map.empty[Double, mutable.ArrayBuffer[Seq[Predicate]]]
      rulesImpl(names, List.empty[Predicate], rmap)
      rmap.toMap
    }

    def predictLeafId(features: SparkVector): Int = {
      if (self.isLeaf) self.id else {
        val split = self.split.get
        if (split.featureType == Continuous) {
          if (features(split.feature) <= split.threshold) {
            self.leftNode.get.predictLeafId(features)
          } else {
            self.rightNode.get.predictLeafId(features)
          }
        } else {
          if (split.categories.contains(features(split.feature))) {
            self.leftNode.get.predictLeafId(features)
          } else {
            self.rightNode.get.predictLeafId(features)
          }
        }
      }
    }
  }

  object ClusteringNode {
    implicit def fromNode(self: Node): ClusteringNode = new ClusteringNode(self)
  }
}

class ClusteringTreeModel(self: DecisionTreeModel) extends Serializable {
  import ClusteringTreeModel.Predicate
  import infra.ClusteringNode._

  def predictLeafId(features: SparkVector): Int = self.topNode.predictLeafId(features)

  def nodeIterator: Iterator[Node] = self.topNode.nodeIterator

  def rules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] =
    self.topNode.rules(names)
}

object ClusteringTreeModel {
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

  def defaultName(idx: Int): String = s"f_$idx"

  implicit def fromDTM(self: DecisionTreeModel): ClusteringTreeModel =
    new ClusteringTreeModel(self)
}
