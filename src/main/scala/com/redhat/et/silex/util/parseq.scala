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
