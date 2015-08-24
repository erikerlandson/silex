/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2015 Red Hat, Inc.
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
 * limitations under the License.c
 */


package com.redhat.et.silex.rdd.multiplex

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging, Partition, TaskContext, 
                         Dependency, NarrowDependency, OneToOneDependency}

import com.redhat.et.silex.rdd.util

class MultiplexRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {
  def multiplex[U :ClassTag](n: Int, f: Iterator[T] => Seq[U]): Seq[RDD[U]] = {
    require(n >= 0, "expected sequence length must be >= 0")
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(
      itr => {
        val r = fc(itr)
        require(r.length == n, s"multiplexed sequence was not expected length $n")
        Iterator.single(r)
      },
      true)
    Vector.tabulate(n)(j => mux.mapPartitions(itr => Iterator.single(itr.next()(j)), true))
  }

  def multiplex[U1 :ClassTag, U2 :ClassTag](f: Iterator[T] => (U1, U2)): (RDD[U1], RDD[U2]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr)), true)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1), true)
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2), true)
    (mux1, mux2)
  }

  def multiplex[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](f: Iterator[T] => (U1, U2, U3)): 
      (RDD[U1], RDD[U2], RDD[U3]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr)), true)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1), true)
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2), true)
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3), true)
    (mux1, mux2, mux3)
  }
}

object implicits {
  import scala.language.implicitConversions
  implicit def rddToMultiplexRDD[T :ClassTag](rdd: RDD[T]) = new MultiplexRDDFunctions(rdd)
}
