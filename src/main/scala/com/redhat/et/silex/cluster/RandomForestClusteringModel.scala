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

import org.apache.commons.lang3.reflect.FieldUtils

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector => SparkVector }

import org.apache.spark.mllib.tree.model.{ DecisionTreeModel, RandomForestModel }

class RandomForestClusteringModel(self: RandomForestModel) extends Serializable {
  import ClusteringTreeModel._

  def predictLeafIds(features: SparkVector): Vector[Int] =
    dti.map(_.predictLeafId(features)).toVector

  def predictLeafIds(data: RDD[SparkVector]): RDD[Vector[Int]] = data.map(this.predictLeafIds)

  def countFeatureIndexes: Map[Int, Int] = {
    val raw = dti.flatMap(_.nodeIterator.filter(!_.isLeaf)).map(_.split.get.feature)
    raw.foldLeft(Map.empty[Int, Int]) { (h, x) =>
      val n = h.getOrElse(x, 0)
      h + (x -> (1 + n))
    }
  }

  def histFeatureIndexes: Seq[(Int, Int)] =
    countFeatureIndexes.toVector.sortWith { (a, b) => a._2 > b._2 }

  def countFeatures(names: PartialFunction[Int, String]): Map[String, Int] =
    countFeatureIndexes.map { case (idx, n) =>
      (names.applyOrElse(idx, defaultName), n)
    }

  def histFeatures(names: PartialFunction[Int, String]): Seq[(String, Int)] =
    histFeatureIndexes.map { case (idx, n) =>
      (names.applyOrElse(idx, defaultName), n)
    }

  def rfRules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] = {
    val dtr = dti.map(_.rules(names))
    dtr.foldLeft(Map.empty[Double, Seq[Seq[Predicate]]]) { (m, x) =>
      x.keys.foldLeft(m) { (m, k) =>
        val s = m.getOrElse(k, Seq.empty[Seq[Predicate]])
        m + (k -> (s ++ x(k)))
      }
    }
  }

  private def dti: Iterator[DecisionTreeModel] = {
    val t = FieldUtils.readField(self, "trees", true).asInstanceOf[Array[DecisionTreeModel]]
    t.iterator
  }
}

object RandomForestClusteringModel {
  implicit def fromRFM(self: RandomForestModel): RandomForestClusteringModel =
    new RandomForestClusteringModel(self)
}
