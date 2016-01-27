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
 * limitations under the License.c
 */

package com.redhat.et.silex.rdd

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging, Partition, TaskContext, 
                         Dependency, NarrowDependency, OneToOneDependency}

import org.apache.spark.storage.StorageLevel

class MuxRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {
  import MuxRDDFunctions.defaultSL

  def muxPartitions[U :ClassTag](n: Int, f: Iterator[T] => Seq[U]): Seq[RDD[U]] =
    muxPartitions(n, f, defaultSL)

  def muxPartitions[U :ClassTag](n: Int, f: Iterator[T] => Seq[U],
    persist: StorageLevel): Seq[RDD[U]] = {
    require(n >= 0, "expected sequence length must be >= 0")
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions { itr =>
      val r = fc(itr)
      require(r.length == n, s"multiplexed sequence was not expected length $n")
      Iterator.single(r)
    }
    Vector.tabulate(n) { j => mux.mapPartitions { itr => Iterator.single(itr.next()(j)) } }
  }

  def muxPartitions[U1 :ClassTag, U2 :ClassTag](f: Iterator[T] => (U1, U2)): (RDD[U1], RDD[U2]) =
    muxPartitions(f, defaultSL)

  def muxPartitions[U1 :ClassTag, U2 :ClassTag](f: Iterator[T] => (U1, U2),
    persist: StorageLevel): (RDD[U1], RDD[U2]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr))).cache()
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    (mux1, mux2)
  }

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](f: Iterator[T] => (U1, U2, U3)): 
      (RDD[U1], RDD[U2], RDD[U3]) =
    muxPartitions(f, defaultSL)

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](f: Iterator[T] => (U1, U2, U3),
    persist: StorageLevel): 
      (RDD[U1], RDD[U2], RDD[U3]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr)))
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    (mux1, mux2, mux3)
  }

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](f: Iterator[T] => (U1, U2, U3, U4)): (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    muxPartitions(f, defaultSL)

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](f: Iterator[T] => (U1, U2, U3, U4),
    persist: StorageLevel): (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr)))
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    val mux4 = mux.mapPartitions(itr => Iterator.single(itr.next._4))
    (mux1, mux2, mux3, mux4)
  }

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](f: Iterator[T] => (U1, U2, U3, U4, U5)): 
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    muxPartitions(f, defaultSL)

  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](f: Iterator[T] => (U1, U2, U3, U4, U5), persist: StorageLevel):
  (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr)))
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    val mux4 = mux.mapPartitions(itr => Iterator.single(itr.next._4))
    val mux5 = mux.mapPartitions(itr => Iterator.single(itr.next._5))
    (mux1, mux2, mux3, mux4, mux5)
  }

  def flatMuxPartitions[U :ClassTag](n: Int, f: Iterator[T] => Seq[TraversableOnce[U]]):
      Seq[RDD[U]] = flatMuxPartitions(n, f, defaultSL)

  def flatMuxPartitions[U :ClassTag](n: Int, f: Iterator[T] => Seq[TraversableOnce[U]],
    persist: StorageLevel):
      Seq[RDD[U]] = {
    require(n >= 0, "expected sequence length must be >= 0")
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions { itr =>
      val r = fc(itr)
      require(r.length == n, s"multiplexed sequence was not expected length $n")
      Iterator.single(r)
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => itr.next()(j).toIterator } }
  }

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2])):
      (RDD[U1], RDD[U2]) = flatMuxPartitions(f, defaultSL)

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr))).persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    (mux1, mux2)
  }

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3])):
      (RDD[U1], RDD[U2], RDD[U3]) = flatMuxPartitions(f, defaultSL)

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr))).persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    (mux1, mux2, mux3)
  }

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4])): (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    flatMuxPartitions(f, defaultSL)

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr))).persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    val mux4 = mux.mapPartitions(itr => itr.next._4.toIterator)
    (mux1, mux2, mux3, mux4)
  }

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5])): (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    flatMuxPartitions(f, defaultSL)

  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) = {
    val fc = util.clean(self.context, f)
    val mux = self.mapPartitions(itr => Iterator.single(fc(itr))).persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    val mux4 = mux.mapPartitions(itr => itr.next._4.toIterator)
    val mux5 = mux.mapPartitions(itr => itr.next._5.toIterator)
    (mux1, mux2, mux3, mux4, mux5)
  }

  def samplingMuxer[U](n: Int): Iterator[U] => Seq[Seq[U]] = {
    (data: Iterator[U]) => {
      val samples = Vector.fill(n) { scala.collection.mutable.ArrayBuffer.empty[U] }
      data.foreach { e => samples(scala.util.Random.nextInt(n)) += e }
      samples
    }
  }

  def sampleMux(n: Int): Seq[RDD[T]] = {
    flatMuxPartitions(n, samplingMuxer[T](n))
  }

  def sampleNoMux(n: Int, seed: Long = 42L): Seq[RDD[T]] = {
    Vector.tabulate(n) { j =>
      self.mapPartitions { data =>
        scala.util.Random.setSeed(seed)
        data.filter { unused => scala.util.Random.nextInt(n) == j }
      }
    }
  }
}

object MuxRDDFunctions {
  import scala.language.implicitConversions
  implicit def rddToMuxRDD[T :ClassTag](rdd: RDD[T]): MuxRDDFunctions[T] = new MuxRDDFunctions(rdd)

  private val defaultSL = StorageLevel.MEMORY_ONLY

  def benchmark[T](label: String)(blk: => T) = {
    val t0 = System.nanoTime
    val t = blk
    val sec = (System.nanoTime - t0) / 1e9
    println(f"Run time for $label = $sec%.1f"); System.out.flush
    t
  }
}
