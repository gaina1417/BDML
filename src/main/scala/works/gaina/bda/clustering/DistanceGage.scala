package works.gaina.bda.clustering

import breeze.linalg.{sum, DenseVector => BDV}
import breeze.numerics.{abs, log, pow, sqrt}

/**
  * Created by gaina on 10/17/16.
  */
object DistanceGage {

  def normL1(v: BDV[Double]): Double = {
    sum(v.map(abs(_)))
  }

  def normSquaredL2(v: BDV[Double]): Double = {
    sum(v.map(pow(_, 2)))
  }

  def normLp(v: BDV[Double], p: Double): Double =
    pow(sum(v.map(abs(_)).map(pow(_, p))), 1 / p)

  def normL2(v: BDV[Double]): Double = {
    sqrt(normSquaredL2(v))
  }

  def getCityBlock(v1: BDV[Double], v2: BDV[Double]): Double =
    normL1(v1 - v2)

  def getSquaredEuclidean(v1: BDV[Double], v2: BDV[Double]): Double =
    normSquaredL2(v1 - v2)

  def getEuclidean(v1: BDV[Double], v2: BDV[Double]): Double =
    normL2(v1 - v2)

  def getMinkowski(v1: BDV[Double], v2: BDV[Double], p: Int): Double =
    normLp(v1 - v2, p)

  def getChebyshev(v1: BDV[Double], v2: BDV[Double]): Double =
    (v1 - v2).map(abs(_)).max

  def pairMin(v1: BDV[Double], v2: BDV[Double]): BDV[Double] = {
    val result = BDV.zeros[Double](v1.length)
    for (i <- 0 to v1.length - 1)
      if (v1(i) <= v2(i))
        result(i) = v1(i)
      else
        result(i) = v2(i)
    result
  }

  def pairMax(v1: BDV[Double], v2: BDV[Double]): BDV[Double] = {
    val result = BDV.zeros[Double](v1.length)
    for (i <- 0 to v1.length - 1)
      if (v1(i) <= v2(i))
        result(i) = v2(i)
      else
        result(i) = v1(i)
    result
  }

  def getSorencen(v1: BDV[Double], v2: BDV[Double]): Double =
    getCityBlock(v1, v2) / sum(v1 + v1)

  def getSoergel(v1: BDV[Double], v2: BDV[Double]): Double =
    getCityBlock(v1, v2) / sum(pairMax(v1, v2))

  def getKulczynski(v1: BDV[Double], v2: BDV[Double]): Double =
    getCityBlock(v1, v2) / sum(pairMin(v1, v2))

  def getCanberra(v1: BDV[Double], v2: BDV[Double]): Double =
    sum((v1 - v2).map(abs(_)) :/ (v1 + v2))

  def getLorentzian(v1: BDV[Double], v2: BDV[Double]): Double = {
    val v = (v1-v2).map(1 + abs(_))
    sum(v.map(log(_)))
  }

  def getIntersection(v1: BDV[Double], v2: BDV[Double]): Double =
    sum(pairMin(v1, v2))

  def getWaveHedges(v1: BDV[Double], v2: BDV[Double]): Double =
    0.0

  def getCzekanowski(v1: BDV[Double], v2: BDV[Double]): Double =
    2 * sum(pairMin(v1, v2)) / sum(v1 + v2)

  def getMotykaS(v1: BDV[Double], v2: BDV[Double]): Double =
    sum(pairMin(v1, v2)) / sum(v1 + v2)

  def getMotykaD(v1: BDV[Double], v2: BDV[Double]): Double =
    sum(pairMax(v1, v2)) / sum(v1 + v2)

  def getKulczynskiS(v1: BDV[Double], v2: BDV[Double]): Double =
    sum(pairMin(v1, v2)) / getCityBlock(v1, v2)

  def getRuzicka(v1: BDV[Double], v2: BDV[Double]): Double =
    sum(pairMin(v1, v2)) / sum(pairMax(v1, v2))

  def getTanimoto(v1: BDV[Double], v2: BDV[Double]): Double =
    (sum(pairMax(v1, v2)) - sum(pairMin(v1, v2))) / sum(pairMax(v1, v2))

  def getKumarHassebrook(v1: BDV[Double], v2: BDV[Double]): Double = {
    val sumOfPairProduct = sum(v1 :* v2)
    sumOfPairProduct / (normSquaredL2(v1) + normSquaredL2(v1) + sumOfPairProduct)
  }

  def getJaccardS(v1: BDV[Double], v2: BDV[Double]): Double = {
    val innerProduct = getInnerProduct(v1, v2)
    innerProduct / (normSquaredL2(v1) + normSquaredL2(v2) - innerProduct)
  }

  def getJaccardD(v1: BDV[Double], v2: BDV[Double]): Double = {
    val sumOfPairProduct = sum(v1 :* v2)
    sumOfPairProduct / (normSquaredL2(v1) + normSquaredL2(v2) - sumOfPairProduct)
  }

  def getDiceS(v1: BDV[Double], v2: BDV[Double]): Double =
    2 * sum(v1 :* v2) / (normSquaredL2(v1) + normSquaredL2(v2))

  def getDiceD(v1: BDV[Double], v2: BDV[Double]): Double =
    normSquaredL2(v1 - v2) / (normSquaredL2(v1) + normSquaredL2(v2))

  def getInnerProduct(v1: BDV[Double], v2: BDV[Double]): Double =
    v1 dot v2

  def getHarmonicMean(v1: BDV[Double], v2: BDV[Double]): Double =
    sum((v1 :* v2) :/ (v1 + v2))

  def getCosine(v1: BDV[Double], v2: BDV[Double]): Double =
    getInnerProduct(v1, v2) / (normL2(v1) * normL2(v2))

}
