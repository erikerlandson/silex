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

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.tree.RandomForest

import com.redhat.et.silex.feature.extractor.Extractor

import com.redhat.et.silex.sample.iid.implicits._
import com.redhat.et.silex.util.vectors.implicits._
import ClusteringRandomForestModel._

case class RandomForestCluster[T](
  extractor: T => Seq[Double],
  categoryInfo: Map[Int, Int],
  syntheticSS: Int,
  rfNumTrees: Int,
  rfMaxDepth: Int,
  rfMaxBins: Int,
  clusterK: Int,
  clusterMaxIter: Int,
  clusterEps: Double,
  clusterFractionEps: Double,
  clusterSS: Int,
  clusterThreads: Int,
  seed: Long) extends Serializable with Logging {

  require(categoryInfo.valuesIterator.forall(_ > 0), "category counts must be > 0")
  require(syntheticSS >= 0, "syntheticSS must be >= 0")
  require(rfNumTrees > 0, "rfNumTrees must be > 0")
  require(rfMaxDepth > 0, "rfMaxDepth must be > 0")
  require(rfMaxBins > 1, "rfMaxBins must be > 1")
  // clustering parameters checked by KMedoids class

  def setExtractor(extractorNew: T => Seq[Double]) = this.copy(extractor = extractorNew)

  def setCategoryInfo(categoryInfoNew: Map[Int, Int]) = this.copy(categoryInfo = categoryInfoNew)

  def setSyntheticSS(syntheticSSNew: Int) = this.copy(syntheticSS = syntheticSSNew)

  def setRfNumTrees(rfNumTreesNew: Int) = this.copy(rfNumTrees = rfNumTreesNew)

  def setRfMaxDepth(rfMaxDepthNew: Int) = this.copy(rfMaxDepth = rfMaxDepthNew)

  def setRfMaxBins(rfMaxBinsNew: Int) = this.copy(rfMaxBins = rfMaxBinsNew)

  def setClusterK(clusterKNew: Int) = this.copy(clusterK = clusterKNew)

  def setClusterMaxIter(clusterMaxIterNew: Int) = this.copy(clusterMaxIter = clusterMaxIterNew)

  def setClusterEps(clusterEpsNew: Double) = this.copy(clusterEps = clusterEpsNew)

  def setClusterFractionEps(clusterFractionEpsNew: Double) =
    this.copy(clusterFractionEps = clusterFractionEpsNew)

  def setClusterSS(clusterSSNew: Int) = this.copy(clusterSS = clusterSSNew)

  def setClusterThreads(clusterThreadsNew: Int) = this.copy(clusterThreads = clusterThreadsNew)

  def setSeed(seedNew: Long) = this.copy(seed = seedNew)

  def run(data: RDD[T]) = {

    scala.util.Random.setSeed(seed)

    val spark = data.sparkContext

    logInfo(s"Extracting feature data...")
    val fvData = data.map(extractor)

    logInfo(s"Assembling synthetic data from marginal distributions...")
    val sss = if (syntheticSS > 0) syntheticSS else fvData.count.toInt
    val synData = fvData.iidFeatureSeqRDD(sss)

    logInfo(s"Assembling RF model training set...")
    val fvVec = fvData.map(_.toSpark)
    val synVec = synData.map(_.toSpark)
    val trainVec = new org.apache.spark.rdd.UnionRDD(spark, List(fvVec, synVec))
    val trainLab = new org.apache.spark.rdd.UnionRDD(spark,
      List(fvVec.map(_.toLabeledPoint(1.0)), synVec.map(_.toLabeledPoint(0.0))))

    logInfo(s"Training RF model...")
    val rfModel = RandomForest.trainClassifier(
      trainLab,
      2,
      categoryInfo,
      rfNumTrees,
      "auto",
      "gini",
      rfMaxDepth,
      rfMaxBins,
      scala.util.Random.nextInt)

    val kMedoids = KMedoids(RandomForestCluster.leafIdDist)
      .setK(clusterK)
      .setMaxIterations(clusterMaxIter)
      .setEpsilon(clusterEps)
      .setFractionEpsilon(clusterFractionEps)
      .setSampleSize(clusterSS)
      .setNumThreads(clusterThreads)
      .setSeed(scala.util.Random.nextInt)

    logInfo(s"Clustering leaf id vectors...")
    val clusterModel = kMedoids.run(fvVec.map(rfModel.predictLeafIds))

    logInfo(s"Completed RF clustering model")
    new RandomForestClusterModel(
      extractor,
      rfModel,
      clusterModel)
  }
}

object RandomForestCluster {
  private [cluster] def leafIdDist(a: Vector[Int], b: Vector[Int]): Double = {
    var d = 0
    var j = 0
    val n = a.length
    while (j < n) {
      if (a(j) != b(j)) d += 1
      j += 1
    }
    d.toDouble
  }

  private [cluster] object default {
    def categoryInfo[T](extractor: T => Seq[Double]) = extractor match {
      case e: Extractor[_] => e.categoryInfo.toMap
      case _ => Map.empty[Int, Int]
    }
    def syntheticSS = 0
    def rfNumTrees = 10
    def rfMaxDepth = 5
    def rfMaxBins = 5
    def clusterK = 0
    def clusterMaxIter = KMedoids.default.maxIterations
    def clusterEps = KMedoids.default.epsilon
    def clusterFractionEps = KMedoids.default.fractionEpsilon
    def clusterSS = KMedoids.default.sampleSize
    def clusterThreads = KMedoids.default.numThreads
    def seed = KMedoids.default.seed
  }

  def apply[T](extractor: T => Seq[Double]): RandomForestCluster[T] = RandomForestCluster(
    extractor,
    default.categoryInfo(extractor),
    default.syntheticSS,
    default.rfNumTrees,
    default.rfMaxDepth,
    default.rfMaxBins,
    default.clusterK,
    default.clusterMaxIter,
    default.clusterEps,
    default.clusterFractionEps,
    default.clusterSS,
    default.clusterThreads,
    default.seed)
}
