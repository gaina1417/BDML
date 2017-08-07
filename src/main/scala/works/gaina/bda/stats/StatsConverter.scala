package works.gaina.bda.stats

/**
  * Created by Anatol Gaina on 10/13/16.
  * Trait contains methods for calculation of one - and two- variate descriptive statistics, using sum of elements,
  * sum of squares and element-wise product for two attributes
  *
  * This interface can be used for data from different sources, for which is calculated values, mentioned above.
  */

trait StatsConverter extends Serializable {

  def zeroDataCorrelation = -math.Pi
  def identicalDataCorrelation = math.Pi

  /**
    * Calculate mean of sample
    * @param size - size of a sample
    * @param sum  - sum of all elements of a sample
    * @return     - average value of this sample
    */
  def getMean(size: Int, sum: Double): Double =
    sum / size

  /**
    * Calculate sum of square of deviations from average value of sample
    * @param size     - size of sample
    * @param sum      - sum of all elements of sample
    * @param sumOfSqr - sum of all square of elements of sample
    * @return         - sum of square of deviations from average value of sample
    */
  def getSumOfSquaredDeviations(size: Int, sum: Double, sumOfSqr: Double): Double =
    sumOfSqr - math.pow(sum, 2) / size

  /**
    * Calculate variance of sample
    * @param size     - size of sample
    * @param sum      - sum of all elements of sample
    * @param sumOfSqr - sum of all square of elements of sample
    * @return         - variance of sample
    */
  def getVariance(size: Int, sum: Double, sumOfSqr: Double): Double =
    getSumOfSquaredDeviations(size, sum, sumOfSqr) / (size - 1)

  /**
    * Calculate standard deviation of sample
    * @param variance - variance of sample
    * @return         - standard deviation of sample
    */
  def getStdDev(variance: Double): Double =
    math.sqrt(variance)

  /**
    * Calculate standard deviation of sample
    * @param size     - size of sample
    * @param sum      - sum of all elements of sample
    * @param sumOfSqr - sum of all square of elements of sample
    * @return         - standard deviation of sample
    */
  def getStdDev(size: Int, sum: Double, sumOfSqr: Double): Double =
    math.sqrt(getVariance(size, sum, sumOfSqr))

  /**
    * Calculate covariance of sample
    * @param size           - size of two-variate sample
    * @param sum1           - sum of all elements of 1st attribute of sample
    * @param sum2           - sum of all elements of 2nd attribute of sample
    * @param sumOfProducts  - sum of products of respective pairs of attributes
    * @return               - covariance of 2 attributes
    */
  def getCovariance(size: Int, sum1: Double, sum2: Double, sumOfProducts: Double): Double =
    (sumOfProducts - sum1 * sum2 / size) / (size -1)

  /**
    * Calculate Pearson coefficient of correlation
    * @param covariance - covariance of 2 attributes
    * @param stdDev1    - standard deviation of 1st attribute of sample
    * @param stdDev2    - standard deviation of 2nd attribute of sample
    * @return           - Pearson coefficient of correlation
    */
  def getPearsonCorrelation(covariance: Double, stdDev1: Double, stdDev2: Double) =
    covariance / (stdDev1 * stdDev2)

  /**
    * Calculate Pearson coefficient of correlation
    * @param size          - size of two-variate sample
    * @param sum1          - sum of all elements of 1st attribute of sample
    * @param sumOfSqr1     - sum of all square of elements of 1st attribute of sample
    * @param sum2          - sum of all elements of 2nd attribute of sample
    * @param sumOfSqr2     - sum of all square of elements of 2nd attribute of sample
    * @param sumOfProducts - sum of products of respective pairs of attributes
    * @return              - Pearson coefficient of correlation
    */
  def getPearsonCorrelation(size: Int, sum1: Double, sumOfSqr1: Double, sum2: Double, sumOfSqr2: Double, sumOfProducts: Double): Double = {
    val stdDev1 = getStdDev(size, sum1, sumOfSqr1)
    val stdDev2 = getStdDev(size, sum2, sumOfSqr2)
    if ((stdDev1 > 0.0) && (stdDev2 > 0.0))
      getCovariance(size, sum1, sum2, sumOfProducts) / (stdDev1 * stdDev2)
    //    getCovariance(size, sum1, sum2, sumOfProducts) / (getStdDev(size, sum1, sumOfSqr1) * getStdDev(size, sum2, sumOfSqr2))
    else
      zeroDataCorrelation
  }

  /**
    * Calculate Pearson coefficient of correlation
    * @param covariance - covariance of 2 attributes
    * @param size       - size of two-variate sample
    * @param stdDev1    - standard deviation of 1st attribute of sample
    * @param sum2       - sum of all elements of 2nd attribute of sample
    * @param sumOfSqr2  - sum of all square of elements of 2nd attribute of sample
    * @return           - Pearson coefficient of correlation
    */
  def getPearsonCorrelation(covariance: Double, size: Int, stdDev1: Double, sum2: Double, sumOfSqr2: Double) =
    covariance / (stdDev1 * getStdDev(size, sum2, sumOfSqr2))

  def tStatForPearsonCorrelation(correlation: Double, sampleSize: Int) = {
    var result = zeroDataCorrelation
    if (sampleSize >2)
      if (math.abs(correlation) < 1)
        result = correlation / math.sqrt((1 - math.pow(correlation, 2)) * (sampleSize - 2))
      else
        result = identicalDataCorrelation
    result
  }

  def getBinOld(value: Double, min: Double, max: Double, bins: Int): Int = {
    if (value == max)
      bins - 1
    else
      ((value - min) / (max - min) * (bins -1)).toInt
  }

  def getBin(value: Double, min: Double, max: Double, bins: Int): Int =
    ((value - min) / (max - min) * bins).toInt

  def safeGetBin(value: Double, min: Double, max: Double, bins: Int): Int = {
    var result = -1
    if((min < max) && (min <= value) && (value <= max))
      result = getBin(value, min, max, bins)
    else
    if ((min <= value) && (value <= max))
      result = 0
    result
  }

}
