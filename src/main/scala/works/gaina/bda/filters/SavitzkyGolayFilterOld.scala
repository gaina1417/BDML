package works.gaina.bda.filters

/**
  * Created by gaina on 3/10/17.
  */
import scala.collection.mutable.ArrayBuffer

/**
  * Object filters efficiently data like empirical density and can be used for smoothing of histograms
  *
  * Another using - calculation of numerical derivatives
  */
object SavitzkyGolayFilterOld {
  // Smoothing - PseudoDerivative order 0 2/3 (quadratic / cubic)
  private val convolution0235 = Array(         -3, 12, 17, 12, -3,           35)
  private val convolution0237 = Array(      -2, 3,  6,  7,  6,  3, -2,       21)
  private val convolution0239 = Array(-21, 14, 39, 54, 59, 54, 39, 14, -21, 231)
  // Smoothing - PseudoDerivative order 0 4/5 (quartic/quintic)
  private val convolution0457 = Array(      5, -30,  75, 131,  75, -30,   5,     231)
  private val convolution0459 = Array(15, -55,  30, 135, 179, 135,  30, -55, 15, 429)

  // 1st Derivative 1/2 (linear/quadratic)
  private val convolution1125 = Array(        -2, -1, 0, 1, 2,       10)
  private val convolution1127 = Array(    -3, -2, -1, 0, 1, 2, 3,    28)
  private val convolution1129 = Array(-4, -3, -2, -1, 0, 1, 2, 3, 4, 60)
  // 1st Derivative 3/4 cubic/quartic
  private val convolution1345 = Array(             1,   -8, 0,   8,  -1,            12)
  private val convolution1347 = Array(      22,  -67,  -58, 0,  58,  67, -22,       252)
  private val convolution1349 = Array(86, -142, -193, -126, 0, 126, 193, 142, -86, 1188)

  // 2nd Derivative 2/3 (quadratic / cubic)
  private val convolution2235 = Array(        2,  -1,  -2,  -1,  2,          7)
  private val convolution2237 = Array(    5,  0,  -3,  -4,  -3,  0, 5,      42)
  private val convolution2239 = Array(28, 7, -8, -17, -20, -17, -8, 7, 28, 462)
  // 2nd Derivative 4/5 quartic/quintic
  private val convolution2455 = Array(            -1,   16,  -30,   16,  -1,               12)
  private val convolution2457 = Array(      -13,  67,  -19,  -70,  -19,  67,  -13,        132)
  private val convolution2459 = Array(-126, 371, 151, -211, -370, -211, 151,  371, -126, 1716)
  // 3rd Derivative 3/4 (cubic/quartic)
  private val convolution3345 = Array(            1, 2,  0,  -2,  1,       2)
  private val convolution3347 = Array(    -1,  1, 1, 0, -1,  -1,  1,       6)
  private val convolution3349 = Array(-14, 7, 13, 9, 0, -9, -13, -7, 14, 198)
  // 3rd Derivative 5/6 (quintic/sextic)
  private val convolution3567 = Array(       1,  -8,  13, 0,  -13,    8,   -1,          8)
  private val convolution3569 = Array(100 -457, 256, 459, 0, -459, -256, -457, -100, 1144)

  // 4th Derivative 4/5 quartic/quintic
  private val convolution4457 = Array(      3,  -7, 1,  6, 1,  -7,  3,       11)
  private val convolution4459 = Array(14, -21, -11, 9, 18, 9, -11, -21, 14, 143)

  private val parsSeparator = "*"
  private val degreesSeparator = "/"

  val convolutionsBasis = scala.collection.mutable.Map[String, Array[Double]]()

  def makeKey(derivativeOrder: Int, degrees: (Int, Int), windowSize:Int): String = {
    val key = new StringBuilder
    if (validPars(derivativeOrder, degrees, windowSize))
      key.append(derivativeOrder.toString + parsSeparator +
        degrees._1.toString + degreesSeparator + degrees._2.toString + parsSeparator +
        windowSize.toString)
    key.toString
  }

  /**   */
  def calculateConvolutionCoefficients(convolution: Array[Int]): Array[Double] = {
    val result = new ArrayBuffer[Double]
    val pivot = convolution.last.toDouble
    for (i <- 0 to convolution.size-2)
      result.append(convolution(i).toDouble / pivot)
    result.toArray
  }

  /**   */
  def addToConvolutionsBasis(derivativeOrder: Int, degrees: (Int, Int), windowSize: Int, convolution: Array[Int]): Unit = {
    if (validPars(derivativeOrder, degrees, windowSize))
      convolutionsBasis +=
        makeKey(derivativeOrder, degrees, windowSize) -> calculateConvolutionCoefficients(convolution)
  }

  /**   */
  def fillconvolutionBasis: Unit = {
    addToConvolutionsBasis(0, (2, 3), 5, convolution0235)
    addToConvolutionsBasis(0, (2, 3), 7, convolution0237)
    addToConvolutionsBasis(0, (2, 3), 9, convolution0239)
    addToConvolutionsBasis(0, (4, 5), 7, convolution0457)
    addToConvolutionsBasis(0, (4, 5), 9, convolution0459)

    addToConvolutionsBasis(1, (1, 2), 5, convolution1125)
    addToConvolutionsBasis(1, (1, 2), 7, convolution1127)
    addToConvolutionsBasis(1, (1, 2), 9, convolution1129)
    addToConvolutionsBasis(1, (3, 4), 5, convolution1345)
    addToConvolutionsBasis(1, (3, 4), 7, convolution1347)
    addToConvolutionsBasis(1, (3, 4), 9, convolution1349)

    addToConvolutionsBasis(2, (2, 3), 5, convolution2235)
    addToConvolutionsBasis(2, (2, 3), 7, convolution2237)
    addToConvolutionsBasis(2, (2, 3), 9, convolution2239)
    addToConvolutionsBasis(2, (4, 5), 5, convolution2455)
    addToConvolutionsBasis(2, (4, 5), 7, convolution2457)
    addToConvolutionsBasis(2, (4, 5), 9, convolution2459)

    addToConvolutionsBasis(3, (3, 4), 5, convolution3345)
    addToConvolutionsBasis(3, (3, 4), 7, convolution3347)
    addToConvolutionsBasis(3, (3, 4), 9, convolution3349)
    addToConvolutionsBasis(3, (5, 6), 7, convolution3567)
    addToConvolutionsBasis(3, (5, 6), 9, convolution3569)

    addToConvolutionsBasis(4, (4, 5), 7, convolution4457)
    addToConvolutionsBasis(4, (4, 5), 9, convolution4459)
  }

  /** Check if order of derivative is in allowed limits
    *
    * @param derivativeOrder
    * @return Valid or nor
    */
  def validDerivativeOrder(derivativeOrder:Int): Boolean =
    (derivativeOrder >= 0) && (derivativeOrder <= 4)

  def validDegrees(derivativeOrder:Int, degrees: (Int, Int)): Boolean = {
    var result: Boolean = false
    degrees match {
      case (1, 2) => if (derivativeOrder == 1)
        result = true
      case (2, 3) => if ((derivativeOrder == 0) || (derivativeOrder == 2))
        result = true
      case (3, 4) => if ((derivativeOrder == 1) || (derivativeOrder == 3))
        result = true
      case (4, 5) => if ((derivativeOrder != 1) && (derivativeOrder != 3))
        result = true
      case (5, 6) => if (derivativeOrder == 4)
        result = true
    }
    result
  }

  /**   */
  def validPars(derivativeOrder:Int, degrees: (Int, Int), windowSize:Int): Boolean = {
    var result: Boolean = validDerivativeOrder(derivativeOrder) &&
      validDegrees(derivativeOrder, degrees)
    val validWindowsSize = new ArrayBuffer[Int]
    if (result) {
      validWindowsSize.append(5,7,9)
/*
      degrees match {
        case (4, 5) => if ((derivativeOrder == 0) || (derivativeOrder == 4))
          validWindowsSize.remove(0)
        case (5, 6) => validWindowsSize.remove(0)
      }
*/
      result = validWindowsSize.contains(windowSize)
    }
    result
  }

  /**
    * Calculate valid limits for given sample size and windows size
    * @param sampleSize - size of sample
    * @param windowSize - size of windows
    * @return tuple with valid shift
    */
  private def getValidLimits(sampleSize:Int, windowSize:Int): (Int, Int, Int) = {
    var result: (Int, Int, Int) = (-1, -1, -1)
    val shift =  (windowSize - 1) / 2
    val i1 = shift
    val i2 = sampleSize - shift
    if ((i1 <= i2) && (i2 > 0))
      result = (i1, i2, shift)
    result
  }

  /** Calculate scalarProduct between convolution coefficients and respective slice of data
    *
    * @param convolutionCoefficients - selected convolution coefficients
    * @param data                    - input data
    * @param node                    - point for which convolution is calculated
    * @param shift                   - shift relative node
    * @return convolution in node
    */
  private def scalarProduct(convolutionCoefficients:Array[Double], data: Array[Double], node: Int, shift: Int): Double = {
    var result: Double = 0
    for (i <- 0 to convolutionCoefficients.size-1)
      result += convolutionCoefficients(i) * data(node - shift + i)
    result
  }

  /**
    * Get convolution coefficients
    * @param derivativeOrder - order of derivative
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @return convolution coefficients
    */
  def getConvolutionCoefficients(derivativeOrder:Int, degrees: (Int, Int), windowSize:Int): Array[Double] = {
    val key = makeKey(derivativeOrder, degrees, windowSize)
    if (convolutionsBasis.contains(key))
      convolutionsBasis.apply(key)
    else
      Array(1.0)
  }

  /**
    * Apply convolution to input data
    * @param convolutionCoefficients - selected convolution coefficients
    * @param data                    -  input data
    * @return
    */
  def apply(convolutionCoefficients:Array[Double], data: Array[Double]): Array[Double] = {
    val result = new ArrayBuffer[Double]
    val limits = getValidLimits(data.size, convolutionCoefficients.size)
    val shift = limits._3
    if (limits._1 != -1) {
      for (i <- limits._1 to limits._2)
        result.append(scalarProduct(convolutionCoefficients, data, i, shift))
    }
    result.toArray
  }

  /**
    * Apply filter to input data
    * @param derivativeOrder - order of derivative
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def apply(derivativeOrder:Int, degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(getConvolutionCoefficients(derivativeOrder, degrees, windowSize), data)

  /**
    * Smooth input data
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def smooth(degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(0, degrees, windowSize, data)

  /**
    * Calculate derivative of 1st order
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def firstDerivative(degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(1, degrees, windowSize, data)

  /**
    * Calculate derivative of 2nd order
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def secondDerivative(degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(2, degrees, windowSize, data)

  /**
    * Calculate derivative of 3rd order
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def thirdDerivative(degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(3, degrees, windowSize, data)

  /**
    * Calculate derivative of 4th order
    * @param degrees         - degrees of convolution coefficients
    * @param windowSize      - width of smoothing window
    * @param data            - data to process
    * @return calculated values of numerical derivative
    */
  def fourthDerivative(degrees: (Int, Int), windowSize:Int, data: Array[Double]): Array[Double] =
    apply(4, degrees, windowSize, data)

}
