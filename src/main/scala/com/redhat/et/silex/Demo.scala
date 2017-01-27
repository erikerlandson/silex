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

object demo {
  import scala.language.postfixOps
  import java.io.ByteArrayInputStream
  import scala.sys.process._

  def histogram[N](data: Seq[N], cumulative: Boolean = false, normalized: Boolean = false)(implicit num: Numeric[N]): Unit = {
    require(data.length > 0)
    val dd = data.map(num.toDouble(_))
    val histstyle = (cumulative, normalized) match {
      case (false, false) => "frequency"
      case (false, true) => {
        println("cumulative normalized histograms not supported by current gnuplot: using frequency")
        "frequency"
      }
      case (true, false) => "cumulative"
      case (true, true) => "cnormal"
    }
    if (cumulative) "cumulative" else "frequency"
    val (dmin, dmax) = (dd.min, dd.max)
    val bsden = math.min(10.0, dd.distinct.length.toDouble)
    val binsize = (dmax-dmin)/bsden
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream(dd.mkString("\n").getBytes("UTF-8"))
    (List("/usr/local/bin/feedgnuplot", "--unset", "grid", "--histogram", "0", "--histstyle", s"$histstyle", "--binwidth", s"$binsize", "--ymin", "0", "--terminal", "dumb 70,35", "--exit") #< inputStream)!
  }

  def scatter[N1, N2](data: Seq[(N1, N2)])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): Unit = {
    val dd = data.map { case (d1, d2) => ((num1.toDouble(d1), num2.toDouble(d2))) }
    val dc = dd.map { case (d1, d2) => s"$d1, $d2" }
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream((dc.mkString("\n") + "\n").getBytes("UTF-8"))
    (List("/usr/local/bin/feedgnuplot", "--points", "--terminal", "dumb 70,35", "--unset", "grid", "--domain", "--exit") #< inputStream)!
  }

  def scatter[N1, N2](xdata: Seq[N1], ydata: Seq[N2])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): Unit = {
    require(xdata.length == ydata.length)
    scatter(xdata.zip(ydata))
  }

  def gaussian_mixture[N1, N2](n: Int, centers: (N1, N2)*)(implicit
      num1: Numeric[N1], num2: Numeric[N2]): Seq[(Double, Double)] = {
    require(n >= 0)
    require(centers.length > 0)
    val nc = centers.length
    val cdd = centers.map { case (d1, d2) => ((num1.toDouble(d1), num2.toDouble(d2))) }
    Vector.fill[(Double, Double)](n) {
      val x = scala.util.Random.nextGaussian()
      val y = scala.util.Random.nextGaussian()
      val (xc, yc) = cdd(scala.util.Random.nextInt(nc))
      (xc + x, yc + y)
    }
  }
}
