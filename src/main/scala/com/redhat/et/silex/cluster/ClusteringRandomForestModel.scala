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

class ClusteringRandomForestModel(self: RandomForestModel) extends Serializable {
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

  def rules(
    names: PartialFunction[Int, String],
    catInfo: PartialFunction[Int, Int]): Map[Double, Seq[Seq[Predicate]]] = {
    val dtr = dti.map(_.rules(names, catInfo))
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

object ClusteringRandomForestModel {
  implicit def fromRFM(self: RandomForestModel): ClusteringRandomForestModel =
    new ClusteringRandomForestModel(self)
}


class test(spark: org.apache.spark.SparkContext) {
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.mllib.tree.RandomForest
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.mllib.linalg.{DenseVector => DenseSparkVec}
  import org.apache.spark.mllib.regression.LabeledPoint
  import scala.util.Random

  import org.apache.spark.rdd.RDD

  val names = Vector("foo", "bar")
  val catInfo = Map((1 -> 2))

  val raw = Vector.fill(1000) { Array.fill(2) { Random.nextInt(2).toDouble } }

  val x = new DenseSparkVec(Array(0.0, 1.0))

  def test(sc: org.apache.spark.SparkContext) = {
    val y = (x: Array[Double]) =>
      if (x(0) > 0.0  &&  x(1) > 0.0) 1.0 else 0.0

    val data = sc.parallelize(raw)
    val trainData = data.map { x => LabeledPoint(y(x), new DenseSparkVec(x)) }

    val maxBins = 2

    val numClasses = 2

    val featureSubsetStrategy = "auto"
    val impurity = "gini"

    val numTrees = 5
    val maxDepth = 3

    RandomForest.trainClassifier(
      trainData,
      numClasses,
      catInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins,
      73)
  }

  lazy val t = test(spark)
}
