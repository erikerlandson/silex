
object reservoir {
  import scala.reflect.ClassTag
  import org.apache.commons.math3.util.CombinatoricsUtils._
  import com.redhat.et.silex.util.OptionalArg

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

  def ksD(R: Int, j: Int) = {
    def nD(R: Int, j: Int, n0: Int, n1: Int) = {
      val d1 = cdf(R, j)_
      val d2 = cdfG(R, j)_
      (n0 to n1).iterator.map(k => math.abs(d1(k) - d2(k))).max
    }
    var nprv = 0
    var ncur = R * 2
    var prvD = -2.0
    var curD = -1.0
    while (curD != prvD) {
      prvD = curD
      curD = math.max(curD, nD(R, j, nprv, ncur))
      nprv = ncur
      ncur = (ncur * 1.5).toInt
    }
    curD
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

  def dropFunction[T :ClassTag](itr: Iterator[T]) = {
    val arrayClass = Array.empty[T].iterator.getClass
    val arrayBufferClass = scala.collection.mutable.ArrayBuffer.empty[T].iterator.getClass
    val vectorClass = Vector.empty[T].iterator.getClass
    itr.getClass match {
      case `arrayClass` =>
        (data: Iterator[T], n: Int) => data.drop(n)
      case `arrayBufferClass` =>
        (data: Iterator[T], n: Int) => data.drop(n)
      case `vectorClass` =>
        (data: Iterator[T], n: Int) => data.drop(n)
      case _ =>
        (data: Iterator[T], n: Int) => {
          var j = 0
          while (j < n && data.hasNext) {
            data.next()
            j += 1
          }
          data
        }
     }
   }

  def reservoir[T :ClassTag](dataTO: TraversableOnce[T], R: Int) = {
    val data = dataTO.toIterator
    val res = Array.fill(R) { data.next }
    var j = 1 + R
    while (data.hasNext) {
      val v = data.next
      val k = scala.util.Random.nextInt(j)
      if (k < R) res(k) = v
      j += 1
    }
    res.toVector
  }

  val rngEpsilon = 5e-11

  def reservoirFast[T :ClassTag](dataTO: TraversableOnce[T], R: Int, jf: Int = 4) = {
    var data = dataTO.toIterator
    val res = Array.fill(R) { data.next }
    var j = 1 + R
    val jt = R * jf
    // The naive method is faster until (R/j) becomes small enough
    while (data.hasNext && j <= jt) {
      val v = data.next
      val k = scala.util.Random.nextInt(j)
      if (k < R) res(k) = v
      j += 1
    }
    val drop = dropFunction(data)
    // Once gaps become significant, it pays to do gap sampling
    while (data.hasNext) {
      val p = R.toDouble / j.toDouble
      val lnq = math.log1p(-p)
      val u = math.max(scala.util.Random.nextDouble(), rngEpsilon)
      val g = (math.log(u) / lnq).toInt
      data = drop(data, g)
      if (data.hasNext) {
        val v = data.next
        val k = scala.util.Random.nextInt(R)
        res(k) = v
      }
      j += (g + 1)
    }
    res.toVector
  }

  def naiveGaps(n: Int, R: Int) =
    SamplingIterator {
      reservoir((0 until n), R).sorted.iterator.sliding(2).map(s => s(1)-s(0)).toVector
    }

  def fastGaps(n: Int, R: Int) =
    SamplingIterator {
      reservoirFast((0 until n), R).sorted.iterator.sliding(2).map(s => s(1)-s(0)).toVector
    }

  def naiveFastD(n: Int, R: Int, ss: Int) = {
    val naiveDist = naiveGaps(n, R).map(_.toDouble)
    val fastDist = fastGaps(n, R).map(_.toDouble)
    val ks = new org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest()
    ks.kolmogorovSmirnovStatistic(
      Array.fill(ss) { naiveDist.next },
      Array.fill(ss) { fastDist.next })
  }

  def benchmark[U](k: Int, trim: Int = 1)(blk: => U) = {
    Vector.fill(k + 2 * trim) {
      val t0 = System.nanoTime
      blk
      (System.nanoTime - t0) / 1e9
    }.sorted.slice(trim, k + trim)
  }

  class SamplingIterator[T](dataBlock: => Traversable[T]) extends Iterator[T] with Serializable {
    var data = Iterator.continually(()).flatMap { u => dataBlock }

    def hasNext = data.hasNext
    def next = data.next

    override def filter(f: T => Boolean) = new SamplingIterator(data.filter(f).toStream)
    override def map[U](f: T => U) = new SamplingIterator(data.map(f).toStream)
    override def flatMap[U](f: T => scala.collection.GenTraversableOnce[U]) =
      new SamplingIterator(data.flatMap(f).toStream)

    def sample(n: Int) = Vector.fill(n) { data.next }

    def fork = {
      val (dup1, dup2) = data.duplicate
      data = dup1
      new SamplingIterator(dup2.toStream)
    }
  }

  object SamplingIterator {
    def apply[T](data: => Traversable[T]) = new SamplingIterator(data)
    def continually[T](value: => T) = new SamplingIterator(Stream.continually(value))
    implicit class ToSamplingIterator[T](data: TraversableOnce[T]) {
      def toSamplingIterator = new SamplingIterator(data.toStream)
    }
  }
}
