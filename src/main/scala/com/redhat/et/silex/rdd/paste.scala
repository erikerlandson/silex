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

package com.redhat.et.silex.rdd.paste

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

object implicits {
  class PastePartitioner(N: Long, K: Long) extends org.apache.spark.Partitioner {
    require(N >= 0)
    require(K > 0)
    def numPartitions = ((N / K) + (if ((N % K) > 0L) 1L else 0L)).toInt
    def getPartition(key: Any) = (key.asInstanceOf[Long] / K).toInt
  }

  implicit class RDDPasteMethods[T :ClassTag](rddT: RDD[T]) {
    def paste[U :ClassTag](rddU: RDD[U], K: Long = 10000) = {
      val N = rddT.count
      require(rddU.count == N)
      val p = new PastePartitioner(N, K)
      val rddA = rddT.zipWithIndex.map(_.swap).partitionBy(p)
      val rddB = rddU.zipWithIndex.map(_.swap).partitionBy(p)
      rddA.zip(rddB).map(row => (row._1._2, row._2._2))
    }

    def pasteBy[U :ClassTag, A :ClassTag, B :ClassTag](rddU: RDD[U], K: Long = 10000)(fT: T => A, fU: U => B) = {
      val N = rddT.count
      require(rddU.count == N)
      val p = new PastePartitioner(N, K)
      val rddA = rddT.map(fT).zipWithIndex.map(_.swap).partitionBy(p)
      val rddB = rddU.map(fU).zipWithIndex.map(_.swap).partitionBy(p)
      rddA.zip(rddB).map(row => (row._1._2, row._2._2))
    }
  }
}
