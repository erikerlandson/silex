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
package com.redhat.et.silex.rdd.multiplex

import org.scalatest._

import com.redhat.et.silex.testing.PerTestSparkContext

class MuxRDDFunctionsSpec extends FlatSpec with Matchers with PerTestSparkContext {
  import com.redhat.et.silex.testing.matchers._
  import com.redhat.et.silex.rdd.multiplex.MuxRDDFunctions._

  it should "provide muxPartitions for sequence" in {
    val rdd = context.parallelize((1 to 10), 2)
    val mux = rdd.muxPartitions(3, (data: Iterator[Int]) => {
      val j = data.max
      Seq(j, 2*j, 3*j).map(_.toDouble)
    })
    mux.length should be (3)
    mux(0).partitions.length should be (2)
    mux(1).partitions.length should be (2)
    mux(2).partitions.length should be (2)
    mux(0).collect.toSeq should beEqSeq(Seq(5d, 10d))
    mux(1).collect.toSeq should beEqSeq(Seq(10d, 20d))
    mux(2).collect.toSeq should beEqSeq(Seq(15d, 30d))
  }

  it should "provide muxPartitions for 2-tuple" in {
    val rdd = context.parallelize((1 to 10), 2)
    val (mux1, mux2) = rdd.muxPartitions { data: Iterator[Int] =>
      val j = data.max
      (j, j.toString)
    }
    Seq(mux1, mux2).forall(_.partitions.length == 2) should be (true)
    val (res1, res2) = (mux1.collect.toSeq, mux2.collect.toSeq)
    res1 should beEqSeq(Seq(5, 10))
    res2 should beEqSeq(Seq("5", "10"))
  }

  it should "provide muxPartitions for 3-tuple" in {
    val rdd = context.parallelize((1 to 10), 2)
    val (mux1, mux2, mux3) = rdd.muxPartitions { data: Iterator[Int] =>
      val j = data.max
      (j, j.toString, j.toDouble)
    }
    Seq(mux1, mux2, mux3).forall(_.partitions.length == 2) should be (true)
    val (res1, res2, res3) = (mux1.collect.toSeq, mux2.collect.toSeq, mux3.collect.toSeq)
    res1 should beEqSeq(Seq(5, 10))
    res2 should beEqSeq(Seq("5", "10"))
    res3 should beEqSeq(Seq(5.0, 10.0))
  }

  it should "provide muxPartitions for 4-tuple" in {
    val rdd = context.parallelize((1 to 10), 2)
    val (mux1, mux2, mux3, mux4) = rdd.muxPartitions { data: Iterator[Int] =>
      val j = data.max
      (j, j.toString, j.toDouble, j.toLong)
    }
    Seq(mux1, mux2, mux3, mux4).forall(_.partitions.length == 2) should be (true)
    val (res1, res2, res3, res4) =
      (mux1.collect.toSeq, mux2.collect.toSeq, mux3.collect.toSeq, mux4.collect.toSeq)
    res1 should beEqSeq(Seq(5, 10))
    res2 should beEqSeq(Seq("5", "10"))
    res3 should beEqSeq(Seq(5.0, 10.0))
    res4 should beEqSeq(Seq(5L, 10L))
  }

  it should "provide muxPartitions for 5-tuple" in {
    val rdd = context.parallelize((1 to 10), 2)
    val (mux1, mux2, mux3, mux4, mux5) = rdd.muxPartitions { data: Iterator[Int] =>
      val j = data.max
      (j, j.toString, j.toDouble, j.toLong, j.toFloat)
    }
    Seq(mux1, mux2, mux3, mux4, mux5).forall(_.partitions.length == 2) should be (true)
    val (res1, res2, res3, res4, res5) =
      (mux1.collect.toSeq, mux2.collect.toSeq, mux3.collect.toSeq, mux4.collect.toSeq,
      mux5.collect.toSeq)
    res1 should beEqSeq(Seq(5, 10))
    res2 should beEqSeq(Seq("5", "10"))
    res3 should beEqSeq(Seq(5.0, 10.0))
    res4 should beEqSeq(Seq(5L, 10L))
    res5 should beEqSeq(Seq(5f, 10f))
  }
}
