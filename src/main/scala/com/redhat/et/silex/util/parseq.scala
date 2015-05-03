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

package com.redhat.et.silex.util.parseq

import scala.collection.parallel.immutable.ParSeq
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import scala.collection.parallel.immutable.ParVector
import scala.collection.parallel.SeqSplitter
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.collection.mutable.ArrayBuffer

class IntensiveSeqSplitter[T](
  vector: Vector[T],
  par: Int,
  var cur: Int,
  end: Int) extends SeqSplitter[T] {

  println("IntensiveSeqSplitter(" + (if (end-cur > 1) s"$cur, ${end-cur}" else s"$cur") + ")")
  System.out.flush

  def hasNext = cur < end
  def next = {
    val r = vector(cur)
    cur += 1
    r
  }
  def remaining = end-cur
  def split = {
    if (remaining <= 1) {
      Seq(this)
    } else {
      val sz = math.max(remaining / par, 1)
      psplit(Vector.fill(remaining / sz)(sz):_*)
    }
  }
  def psplit(sizes: Int*) = {
    val splits = ArrayBuffer.empty[IntensiveSeqSplitter[T]]
    var rem = remaining
    var c = cur
    sizes.foreach { sz =>
      val s = math.min(sz, rem)
      rem -= s
      splits += new IntensiveSeqSplitter(vector, par, c, c + s)
      c += s
    }
    if (rem > 0) splits += new IntensiveSeqSplitter(vector, par, c, c + rem)
    splits
  }
  def dup = new IntensiveSeqSplitter(vector, par, cur, end)
}

class IntensiveParVector[T](
  vector: Vector[T],
  par: Int,
  bisect: Boolean = false) extends ParVector[T](vector) {

  // Configure the requested number of threads
  this.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(par))

  // configure splitter to either use Scala standard bisection or split by 'par'
  override def splitter =
    new IntensiveSeqSplitter(vector, if (bisect) 2 else par, 0, vector.length)
}

object implicits {
  implicit class EnrichParSeq[T](pseq: ParSeq[T]) {
    def withThreads(n: Int) = {
      require(n > 0, "number of threads must be > 0")
      pseq.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(n))
      pseq
    }

    def withThreads(pool: ForkJoinPool) = {
      pseq.tasksupport = new ForkJoinTaskSupport(pool)
      pseq
    }
  }
}
