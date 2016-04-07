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

  def predictLeafIds(features: SparkVector): Vector[Int] = dtv.map(_.predictLeafId(features))

  def predictLeafIds(data: RDD[SparkVector]): RDD[Vector[Int]] = data.map(this.predictLeafIds)

  private def dtv: Vector[DecisionTreeModel] = {
    val t = FieldUtils.readField(self, "trees", true).asInstanceOf[Array[DecisionTreeModel]]
    t.toVector
  }
}

object RandomForestClusteringModel {
  implicit def fromRFM(self: RandomForestModel): RandomForestClusteringModel =
    new RandomForestClusteringModel(self)
}
