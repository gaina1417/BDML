package works.gaina.bda.filters

/**
  * Created by gaina on 6/21/17.
  */
object SavitzkyGolayFilter extends Serializable {

  def MaxCoeffSize = 11

  type SGFCoeffType = (String, String, String, Array[Integer])

  /*
  def ConvolutionCoeff: Array[SGFCoeffType] = (
    ("0", "ROUGH", "LOW", Array(         -3, 12, 17, 12, -3,           35))
    ("0", "", "AVG", Array(      -2, 3,  6,  7,  6,  3, -2,       21))
    ("0", "", "", Array(-21, 14, 39, 54, 59, 54, 39, 14, -21, 231))
  )
 */

  def x: SGFCoeffType = ("", "", "", Array(         -3, 12, 17, 12, -3,           35))
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

}
