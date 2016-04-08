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
  class ClusteringNode(val self: Node) extends AnyVal {
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
      catInfo: PartialFunction[Int, Int],
      pstack: List[Predicate],
      rmap: mutable.Map[Double, mutable.ArrayBuffer[Seq[Predicate]]]) {

      import Predicate._

      if (self.isLeaf) {
        val cat = self.predict.predict
        if (!rmap.contains(cat)) rmap += (cat -> mutable.ArrayBuffer.empty[Seq[Predicate]])
        rmap(cat) += pstack.reverse
      } else {
        val split = self.split.get
        val f = split.feature
        val fname = names.applyOrElse(f, defaultName)
        if (split.featureType == Continuous) {
          val t = split.threshold
          self.leftNode.get.rulesImpl(names, catInfo, LE(fname, t)::pstack, rmap)
          self.rightNode.get.rulesImpl(names, catInfo, GT(fname, t)::pstack, rmap)
        } else {
          val c = split.categories
          self.leftNode.get.rulesImpl(names, catInfo, IN(fname, c)::pstack, rmap)
          val rpred =
            if (catInfo.isDefinedAt(f)) IN(fname, (0 until catInfo(f)).map(_.toDouble).diff(c))
            else NOTIN(fname, c)
          self.rightNode.get.rulesImpl(names, catInfo, rpred::pstack, rmap)
        }
      }
    }

    def rules(
      names: PartialFunction[Int, String],
      catInfo: PartialFunction[Int, Int]): Map[Double, Seq[Seq[Predicate]]] = {
      val rmap = mutable.Map.empty[Double, mutable.ArrayBuffer[Seq[Predicate]]]
      rulesImpl(names, catInfo, List.empty[Predicate], rmap)
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

  def rules(
    names: PartialFunction[Int, String],
    catInfo: PartialFunction[Int, Int]): Map[Double, Seq[Seq[Predicate]]] =
    self.topNode.rules(names, catInfo)
}

object ClusteringTreeModel {
  sealed trait Predicate extends Serializable {
    def feature: String
  }
  object Predicate {
    case class LE(feature: String, threshold: Double) extends Predicate {
      override def toString = s"($feature <= $threshold)"
    }
    case class GT(feature: String, threshold: Double) extends Predicate {
      override def toString = s"($feature > $threshold)"
    }
    case class IN(feature: String, categories: Seq[Double]) extends Predicate {
      override def toString = s"""($feature in [${categories.mkString(",")}])"""
    }
    case class NOTIN(feature: String, categories: Seq[Double]) extends Predicate {
      override def toString = s"""($feature not-in [${categories.mkString(",")}])"""
    }
  }

  def defaultName(idx: Int): String = s"f_$idx"

  implicit def fromDTM(self: DecisionTreeModel): ClusteringTreeModel =
    new ClusteringTreeModel(self)
}
