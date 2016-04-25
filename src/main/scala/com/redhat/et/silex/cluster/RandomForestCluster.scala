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

/**
 * An object for training a Random Forest clustering model on unsupervised data.
 *
 * Data is required to have a mapping into a feature space of type Seq[Double].
 *
 * @param extractor A feature extraction function for data objects
 * @param categoryInfo A map from feature indexes into numbers of categories.  Feature indexes
 * that do not have an entry in the map are assumed to be numeric, not categorical.  Defaults to
 * category-info from [[Extractor]], if the feature extraction function is of this type.  Otherwise
 * defaults to empty, i.e. all numeric features.
 * @param syntheticSS The size of synthetic (margin-sampled) data to be constructed.  Defaults to
 * the size of the input data.
 * @param rfNumTrees The number of decision trees to train in the Random Forest  Defaults to 10.
 * @param rfMaxDepth Maximum decision tree depth.  Defaults to 5.
 * @param rfMaxBins Maximum histogramming bins to use for numeric data.  Defaults to 5.
 * @param clusterK The number of clusters to use when clustering leaf-id vectors.  Defaults to
 * an automatic estimation of a "good" number of clusters.
 * @param clusterMaxIter Maximum clustering refinement iterations to compute.  Defaults to 25.
 * @param clusterEps Halt clustering if clustering metric-cost changes by less than this value.
 * Defaults to 0
 * @param clusterFractionEps Halt clustering if clustering metric-cost changes by this fractional
 * value from previous iteration.  Defaults to 0.0001
 * @param clusterSS If data is larger, use this random sample size.  Defaults to 1000.
 * @param clusterThreads Use this number of threads to accelerate clustering.  Defaults to 1.
 * @param seed A seed to use for RNG.  Defaults to using a randomized seed value.
 */
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

  /** Set a new feature extraction function for input objects
    * @param extractorNew The feature extraction function
    * @return Copy of this instance with new extractor
    */
  def setExtractor(extractorNew: T => Seq[Double]) = this.copy(extractor = extractorNew)

  /** Set a new category info map
    * @param categoryInfoNew New category-info map to use
    * @return Copy of this instance with new category info
    */
  def setCategoryInfo(categoryInfoNew: Map[Int, Int]) = this.copy(categoryInfo = categoryInfoNew)

  /** Set a new synthetic data sample size
    * @param syntheticSSNew New synthetic data size to use
    * @return Copy of this instance with new synthetic data size
    */
  def setSyntheticSS(syntheticSSNew: Int) = this.copy(syntheticSS = syntheticSSNew)

  /** Set a new number of Random Forest trees to train for the model
    * @param rfNumTreesNew New number of trees to use for the RF
    * @return Copy of this instance with new Random Forest size
    */
  def setRfNumTrees(rfNumTreesNew: Int) = this.copy(rfNumTrees = rfNumTreesNew)

  /** Set a new Random Forest maximum tree depth
    * @param rfMaxDepthNew New maximum decision tree depth
    * @return Copy of this instance with new maximum decision tree depth
    */
  def setRfMaxDepth(rfMaxDepthNew: Int) = this.copy(rfMaxDepth = rfMaxDepthNew)

  /** Set a new Random Forest maximum numeric binning value
    * @param rfMaxBinsNew New maximum numeric binning value
    * @return Copy of this instance with new maximum binning value
    */
  def setRfMaxBins(rfMaxBinsNew: Int) = this.copy(rfMaxBins = rfMaxBinsNew)

  /** Set a new target cluster size
    * @param clusterKNew New target cluster number.  Zero sets to automatic determination.
    * @return Copy of this instance with new target cluster size
    */
  def setClusterK(clusterKNew: Int) = this.copy(clusterK = clusterKNew)

  /** Set a new maximum clustering refinement iteration
    * @param clusterMaxIterNew New maximum number of refinement iterations
    * @return Copy of this instance with new maximum iteration
    */
  def setClusterMaxIter(clusterMaxIterNew: Int) = this.copy(clusterMaxIter = clusterMaxIterNew)

  /** Set a new clustering epsilon halting threshold
    * @param clusterEpsNew New epsilon halting threshold
    * @return Copy of this instance with new clustering epsilon threshold
    */
  def setClusterEps(clusterEpsNew: Double) = this.copy(clusterEps = clusterEpsNew)

  /** Set a new clustering fractional epsilon halting threshold
    * @param clusterFractionEpsNew New fractional epsilon value
    * @return Copy of this instance with new fractional epsilon threshold
    */
  def setClusterFractionEps(clusterFractionEpsNew: Double) =
    this.copy(clusterFractionEps = clusterFractionEpsNew)

  /** Set a new clustering sample size
    * @param clusterSSNew New clustering sample size
    * @return Copy of this instance with new sample size
    */
  def setClusterSS(clusterSSNew: Int) = this.copy(clusterSS = clusterSSNew)

  /** Set a new clustering number of threads
    * @param clusterThreadsNew New number of process threads to use
    * @return Copy of this instance with new threading number
    */
  def setClusterThreads(clusterThreadsNew: Int) = this.copy(clusterThreads = clusterThreadsNew)

  /** Set a new RNG seed
    * @param seedNew New RNG seed to use
    * @return Copy of this instance with new RNG seed
    */
  def setSeed(seedNew: Long) = this.copy(seed = seedNew)

  /** Train a Random Forest clustering model from input data
    * @param data The input data objects to cluster
    * @return An RF clustering model of the input data
    */
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

/** Factory functions and implicits for RandomForestCluster */
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

  /** Generate a RandomForestCluster object from a feature extraction function, with all other
    * parameters taking on default values.
    * @param extractor The feature extraction function to use on input objects
    * @return New RF clustering object with defaulted parameters
    */
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

import java.io._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class test extends Serializable {
  import com.redhat.et.silex.feature.extractor._
  import com.redhat.et.silex.rdd.paste.implicits._
  import com.redhat.et.silex.rdd.split.implicits._
  import com.redhat.et.silex.util.buffered

  val spark = com.redhat.et.silex.app.sbtConsole.spark

  val dataDir = "/home/eje/analytics/rj_rpm_data"

  val fields = spark.textFile(s"$dataDir/train.txt").map(_.split(" ").toVector)
  val nodes = spark.textFile(s"$dataDir/nodesclean.txt").map { _.split(" ")(1) }
  val ids = fields.paste(nodes)

  val m = fields.first.length - 1
  val ext = Extractor(m, (v: Vector[String]) => v.map(_.toDouble).tail :FeatureSeq)

  val rfcModel = RandomForestCluster(ext)
    .setClusterK(6)
    .setSyntheticSS(250)
    .run(fields)

  val cid = buffered((ids, rfcModel)) { case (ids, rfcModel) =>
    ids.map(rfcModel.predictWithDistanceBy(_)(x => x))
  }

  val (clusters, outliers) = cid.splitFilter { case (_, dist, _) => dist <= 3 }

  val clusterStr = clusters.map { case (j, _, n) => (j, n) }
    .groupByKey
    .collect
    .map { case (j, nodes) => nodes.toSeq.sorted.mkString("\n") }
    .mkString("\n\n")

  val outlierStr = outliers.collect
    .map { case (_, d,n) => (d, n) }
    .toVector.sorted
    .mkString("\n")
}

class demo1 {
  import com.redhat.et.silex.feature.extractor._
  import com.redhat.et.silex.rdd.paste.implicits._
  import com.redhat.et.silex.rdd.split.implicits._
  import com.redhat.et.silex.util.buffered
  import com.redhat.et.silex.sample.iid.implicits._
  import ClusteringTreeModel._

  val spark = com.redhat.et.silex.app.sbtConsole.spark

  val data = Vector.fill(500) {
    val center = if (scala.util.Random.nextInt(2) == 1)
      Vector(-2.0, -2.0)
    else
      Vector(2.0, 2.0)
    center.map(_ + scala.util.Random.nextGaussian()) :Seq[Double]
  }

  val synth = spark.parallelize(data, 1).iidFeatureSeqRDD(data.length).collect.toVector

  val ext = Extractor(2, (v: Seq[Double]) => v : FeatureSeq)
    .withNames("x1", "x2")

  val rfcModel = RandomForestCluster((v: Seq[Double]) => v)
    .setClusterK(2)
    .setRfMaxDepth(2)
    .run(spark.parallelize(data))

  val rectangles = rfcModel.randomForestModel.rules(ext.names, ext.categoryInfo)(1.0).map(rect)

  val pred = data.map(rfcModel.predictBy(_)(v => (v, v)))

  def rect(rule: Seq[Predicate]) = {
    if (rule.length != 2) throw new Exception("eek!")
    val sr = rule.sortBy(_.feature)
    (sr(0), sr(1)) match {
      case (Predicate.LE("x1", t1), Predicate.LE("x2", t2)) => Seq(-10.0, -10.0, t1, t2)
      case (Predicate.LE("x1", t1), Predicate.GT("x2", t2)) => Seq(-10.0, t2, t1, 10.0)
      case (Predicate.GT("x1", t1), Predicate.LE("x2", t2)) => Seq(t1, -10.0, 10.0, t2)
      case (Predicate.GT("x1", t1), Predicate.GT("x2", t2)) => Seq(t1, t2, 10.0, 10.0)
      case (p1, p2) => throw new Exception(s"($p1, $p2)")
    }
  }

  def dumpdata(fname: String) {
    val dd = Map(
      ("x0" -> synth.map(_(0))),
      ("y0" -> synth.map(_(1))),
      ("x1" -> data.map(_(0))),
      ("y1" -> data.map(_(1)))
    )
    val json = dd
    val out = new PrintWriter(new File(fname))
    out.println(pretty(render(json)))
    out.close()    
  }

  def dumprect(fname: String) {
    val dd = Map(
      ("rect" -> rectangles)
    )
    val json = dd
    val out = new PrintWriter(new File(fname))
    out.println(pretty(render(json)))
    out.close()    
  }

  def dumppred(fname: String) {
    val dd = Map(
      ("x0" -> pred.filter { case (j, _) => j == 0 }.map { case (_, v) => v(0) }),
      ("y0" -> pred.filter { case (j, _) => j == 0 }.map { case (_, v) => v(1) }),
      ("x1" -> pred.filter { case (j, _) => j == 1 }.map { case (_, v) => v(0) }),
      ("y1" -> pred.filter { case (j, _) => j == 1 }.map { case (_, v) => v(1) })
    )
    val json = dd
    val out = new PrintWriter(new File(fname))
    out.println(pretty(render(json)))
    out.close()    
  }
}
