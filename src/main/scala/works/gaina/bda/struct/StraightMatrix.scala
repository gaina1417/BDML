package works.gaina.bda.struct

import works.gaina.bda.utils.NumUtils
import breeze.linalg.{max, min, DenseVector => BDV}

/**
  * Created by gaina on 12/16/16.
  */
class StraightMatrix(rows: Int, cols: Int) extends Serializable with MatrixLinearAccess {

  val size = sizeOfNormalMatrix(rows, cols)
  val body = createEmpty

  def getRows: Int = rows

  def getCols: Int = cols

  def createEmpty: BDV[Double] =
    BDV.zeros[Double](size)

  def isValidRow(i: Int): Boolean =
    NumUtils.inClosedOpenRange(i, 0, rows)

  def isValidCol(j: Int): Boolean =
    NumUtils.inClosedOpenRange(j, 0, cols)

  def isValidCell(i: Int, j: Int): Boolean =
    isValidRow(i) && isValidCol(j)

  def getInd(i: Int, j: Int): Int = {
    if (isValidCell(i, j))
      linearIndexForNormalMatrix(cols, i, j)
    else
      -1
  }

  def put(i: Int, j: Int, value: Double): Boolean = {
    var result: Boolean = true
    val l = getInd(i, j)
    if (l == -1)
      result = false
    else
      body(l) = value
    result
  }

  def haveSameLengthAsRow(bdv: BDV[Double]): Boolean =
    bdv.length == cols

  def haveSameLengthAsCol(bdv: BDV[Double]): Boolean =
    bdv.length == rows

  def isDataEqual(matrix: StraightMatrix) =
    body == matrix.body

  def getRowLimits(i: Int): (Int, Int) = {
    var l1: Int = -1
    var l2: Int = -1
    if (isValidRow(i)) {
      l1 = getInd(i,0)
      l2 = l1 + cols - 1
    }
    (l1, l2)
  }

  def getRowAsBDV(i: Int): BDV[Double] = {
    val lim = getRowLimits(i)
    if (lim != (-1, -1))
      body.slice(lim._1, lim._2)
    else
      null
  }

  def assignValue(value: Double) =
    body := value

  def nullify: Unit =
    assignValue(0)

  def assignVector(v: BDV[Double]) = {
    if (v.length == size)
      body := v
  }

  def assign(matrix: StraightMatrix) =
    assignVector(matrix.body)

  def assignToRow(i: Int, v: BDV[Double]) = {
    if (isValidRow(i)) {
      val lim = getRowLimits(i)
      body(lim._1 to lim._2) := v
    }
  }

  def sumWithRow(i: Int, v: BDV[Double]) = {
    if (isValidRow(i))
      getRowAsBDV(i) + v
    else
      null
  }

  def addToRow(i: Int, v: BDV[Double]): Unit =
    if (isValidRow(i))
      assignToRow(i, sumWithRow(i, v))

  def normByVector(v: BDV[Int]) = {
    for (i <- 0 to rows - 1) {
      val lim = getRowLimits(i)
      for (j <- lim._1 to lim._2)
        body(j) /= v(i)
    }
  }

  def showRowMinMax(i: Int) = {
    if (isValidRow(i)) {
      val v = getRowAsBDV(i)
      val vMin = min(v)
      val vMax = max(v)
      println(s"$i => $vMin, $vMax")
    }
    else
      println("Invalid row number")
  }

  def showRow(i: Int) = {
    if (isValidRow(i)) {
      val v = getRowAsBDV(i)
      println(v)
    }
    else
      println("Invalid row number")
  }

  def show(title: String) = {
    println(s"===============$title==================")
    for (i<-0 to rows-1)
      showRow(i)
    println(s"===============$title==================")
  }

}
