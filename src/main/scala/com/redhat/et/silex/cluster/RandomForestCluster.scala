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

case class RandomForestCluster[T](
  extractor: T => Seq[Double],
  syntheticSS: Int,
  clusterK: Int,
  clusterMaxIter: Int,
  clusterEps: Double,
  clusterFractionEps: Double,
  clusterSS: Int,
  clusterThreads: Int,
  seed: Long) extends Serializable with Logging {
  
}

object RandomForestCluster {
  private[cluster] object default {
    def syntheticSS = 0
    def clusterK = 0
    def clusterMaxIter = KMedoids.default.maxIterations
    def clusterEps = KMedoids.default.epsilon
    def clusterFractionEps = KMedoids.default.fractionEpsilon
    def clusterSS = KMedoids.default.sampleSize
    def clusterThreads = KMedoids.default.numThreads
    def seed = KMedoids.default.seed
  }

  def apply[T](extractor: T => Seq[Double]): RandomForestCluster[T] =
  RandomForestCluster(
    extractor,
    default.syntheticSS,
    default.clusterK,
    default.clusterMaxIter,
    default.clusterEps,
    default.clusterFractionEps,
    default.clusterSS,
    default.clusterThreads,
    default.seed)
}
