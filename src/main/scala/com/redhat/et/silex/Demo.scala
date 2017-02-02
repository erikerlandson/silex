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

  import org.isarnproject.sketches.TDigest

  case class TextPlot(txt: Seq[String]) {
    override def toString = "<text-plot>"
  }

  def show(tp: TextPlot): Unit = println(tp.txt.mkString("\n"))
  def show(tp1: TextPlot, tp2: TextPlot): Unit = {
    val jp = tp1.txt.zip(tp2.txt).map { case(l1, l2) => s"$l1  $l2" }
    println(jp.mkString("\n"))
  }

  def histogram[N](data: Seq[N], cumulative: Boolean = false, normalized: Boolean = false)(implicit num: Numeric[N]): TextPlot = {
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
    val txt = (List("/usr/local/bin/feedgnuplot", "--unset", "grid", "--histogram", "0", "--histstyle", s"$histstyle", "--binwidth", s"$binsize", "--ymin", "0", "--terminal", "dumb 70,35", "--exit") #< inputStream).lines_!.toVector
    TextPlot(txt)
  }

  def scatter[N1, N2](data: Seq[(N1, N2)])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): TextPlot = {
    val dd = data.map { case (d1, d2) => ((num1.toDouble(d1), num2.toDouble(d2))) }
    val dc = dd.map { case (d1, d2) => s"$d1, $d2" }
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream((dc.mkString("\n") + "\n").getBytes("UTF-8"))
    val txt = (List("/usr/local/bin/feedgnuplot", "--points", "--terminal", "dumb 70,35", "--unset", "grid", "--domain", "--exit") #< inputStream).lines_!.toVector
    TextPlot(txt)
  }

  def scatter[N1, N2](xdata: Seq[N1], ydata: Seq[N2])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): TextPlot = {
    require(xdata.length == ydata.length)
    scatter(xdata.zip(ydata))
  }

  def plot[N1, N2](data: Seq[(N1, N2)])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): TextPlot = {
    val dd = data.map { case (d1, d2) => ((num1.toDouble(d1), num2.toDouble(d2))) }
    val dc = dd.map { case (d1, d2) => s"$d1, $d2" }
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream((dc.mkString("\n") + "\n").getBytes("UTF-8"))
    val txt = (List("/usr/local/bin/feedgnuplot", "--lines", "--terminal", "dumb 70,35", "--unset", "grid", "--domain", "--exit") #< inputStream).lines_!.toVector
    TextPlot(txt)
  }

  def tdPlotPDF(td: TDigest, res: Int = 20): TextPlot = {
    val f = pdfFunction(td, res)
    plot((f.xMin to f.xMax by 0.1).map { x => (x, f(x)) })
  }

  def tdPlotCDF(td: TDigest): TextPlot = {
    val (xmin, xmax) = (td.cdfInverse(0), td.cdfInverse(1))
    plot((xmin to xmax by 0.1).map { x => (x, td.cdf(x)) })
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

  case class PDF(pdf: Double => Double, xMin: Double, xMax: Double) extends (Double => Double) {
    def apply(x: Double) = if (x < xMin  || x > xMax) 0.0 else pdf(x)
  }

  def pdfFunction(td: TDigest, res: Int = 20): PDF = {
    require(res > 0)
    val xmin = td.cdfInverse(0)
    val xmax = td.cdfInverse(1)
    val p = 1.0 / res.toDouble
    val xint = (0 until res).map { j =>
      val x0 = td.cdfInverse(j.toDouble / res.toDouble)
      val x1 = td.cdfInverse((j+1).toDouble / res.toDouble)
      (x1, p / (x1 - x0))
    }
    val pdf = (x: Double) => xint.find(x <= _._1).map(_._2).getOrElse(0.0)
    PDF(pdf, xmin, xmax)
  }
}
