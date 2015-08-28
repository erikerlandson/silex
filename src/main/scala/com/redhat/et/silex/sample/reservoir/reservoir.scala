
object reservoir {
  import org.apache.commons.math3.util.CombinatoricsUtils._

  def pmf(R: Int, j: Int)(k: Int): Double = {
    val log = math.log(R) - math.log(j) + 
      binomialCoefficientLog(j, R+1) - binomialCoefficientLog(k+j, R+1)
    math.exp(log)
  }

  def cdf(R: Int, j: Int)(k: Int): Double = {
    val log = binomialCoefficientLog(j-1, R) - binomialCoefficientLog(k+j, R)
    1.0 - math.exp(log)
  }

  def err(R: Int, j: Int)(k: Int) =
    (0 to k).iterator.map(t => pmf(R,j)(t)).sum - cdf(R,j)(k)

  def pmfG(R: Int, j: Int)(k: Int): Double = {
    val p = R.toDouble / j.toDouble
    math.pow(1.0 - p, k) * p
  }

  def cdfG(R: Int, j: Int)(k: Int): Double = {
    val p = R.toDouble / j.toDouble
    1.0 - math.pow(1.0 - p, k + 1)
  }

  def ksD(R: Int, j: Int, n: Int = 10000000) = {
    val d1 = cdf(R, j)_
    val d2 = cdfG(R, j)_
    (0 to n).iterator.map(k => math.abs(d1(k) - d2(k))).max
  }

  def inverseCDFSampler(cdf: Int => Double) = new Iterator[Int] {
    def hasNext = true
    def next = {
      val x = scala.util.Random.nextDouble()
      var u = 2
      while ((cdf(u-1) <= x) && ((2*u - 1) <= Int.MaxValue)) { u *= 2 }
      u = u-1
      if ((cdf(u) <= x)) { throw new Exception(s"failed to bound x= $x") }
      var l = 0
      while (l < u-1) {
        val m = l + ((u-l)/2)
        val y = cdf(m)
        if (y > x) { u = m } else { l = m } 
      }
      l
    }
  }

  val distR = inverseCDFSampler(cdf(10, 100)_)
  val distG = inverseCDFSampler(cdfG(10, 100)_)
  val ks = new org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest()  

  def sampleSize(cdf1: Int => Double, cdf2: Int => Double, sig: Double = 0.01) = {
    var ss = 100
    var pval = 1.0
    
    while (pval > sig) {
      ss = (ss.toDouble * 1.25).toInt
      val sD = Vector.fill(11) {
        val dist1 = inverseCDFSampler(cdf1)
        val dist2 = inverseCDFSampler(cdf2)
        val data1 = dist1.take(ss).map(_.toDouble).toArray
        val data2 = dist2.take(ss).map(_.toDouble).toArray
        ks.kolmogorovSmirnovTest(data1, data2)
      }.sorted
      pval = sD(sD.length / 2)
      println(s"ss= $ss  pval= $pval")
    }
    (ss, pval)
  }

}
