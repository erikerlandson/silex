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

  def histogram[N](data: Seq[N])(implicit num: Numeric[N]): Unit = {
    val dd = data.map(num.toDouble(_))
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream(dd.mkString("\n").getBytes("UTF-8"))
    ("/usr/bin/hist" #< inputStream)!
  }

  def scatter[N1, N2](data: Seq[(N1, N2)])(implicit
      num1: Numeric[N1], num2: Numeric[N2]): Unit = {
    val dd = data.map { case (d1, d2) => ((num1.toDouble(d1), num2.toDouble(d2))) }
    val dc = dd.map { case (d1, d2) => s"$d1,$d2" }
    val inputStream: java.io.InputStream =
      new ByteArrayInputStream(dc.mkString("\n").getBytes("UTF-8"))
    ("/usr/bin/scatter" #< inputStream)!
  }

  
}
