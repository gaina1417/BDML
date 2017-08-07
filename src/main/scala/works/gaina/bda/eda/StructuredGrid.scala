package works.gaina.bda.eda

import works.gaina.bda.stats.StatsConverter
import breeze.linalg.{DenseVector => BDV}
import works.gaina.bda.common.Descriptors.GridType
import works.gaina.bda.struct.StraightMatrix
import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import works.gaina.bda.spark.DataFrameLoader

/**
  * Created by gaina on 12/20/16.
  */
class StructuredGrid(minValues: BDV[Double], maxValues: BDV[Double]) extends Serializable with StatsConverter {

  type CellType = BDV[Int]

  var gridType: GridType.GridType = GridType.UNSETTLED
  var bins: Int = -1
  var binsByAttributes: BDV[Int] = null
  var limitsByAttributes: StraightMatrix = null

  def clearGridType: Unit = {
    gridType = GridType.UNSETTLED
    bins = -1
    binsByAttributes = null
    limitsByAttributes = null
  }

  def setGridUniform(newBins: Int): Unit = {
    clearGridType
    gridType = GridType.UNIFORM
    bins = newBins
  }

  def setGridRegular(newBinsByAttributes: BDV[Int]): Unit = {
    clearGridType
    gridType = GridType.REGULAR
    binsByAttributes = newBinsByAttributes
  }

  def setGridRectangular(newLimitsByAttributes: StraightMatrix): Unit = {
    clearGridType
    gridType = GridType.RECTANGULAR
    limitsByAttributes = newLimitsByAttributes
  }

  def getCellInUniformGrid(data: BDV[Double]): CellType = {
    val result = BDV.zeros[Int](data.size)
    for (i <- 0 to data.size - 1)
      result(i) = getBin(data(i), minValues(i), maxValues(i), bins)
    result
  }

  def getCellInRegularGrid(data: BDV[Double]): CellType = {
    val result = BDV.zeros[Int](data.size)
    for (i <- 0 to data.size - 1)
      result(i) = getBin(data(i), minValues(i), maxValues(i), binsByAttributes(i))
    result
  }

  def getRectangularBinInd(ele: Double, limits: Array[Double]): Int = {
    var ind = Arrays.binarySearch(limits, ele)
    if ((ind < -1) && (math.abs(ind) < limits.size))
      ind = math.abs(ind) - 1
    ind
  }

  def getCellInRectangularGrid(data: BDV[Double]): CellType = {
    val result = BDV.zeros[Int](data.size)
    for (i <- 0 to data.size - 1)
      result(i) = getRectangularBinInd(data(i), limitsByAttributes.getRowAsBDV(i).toArray)
    result
  }

  def getCellInGrid(data: BDV[Double]): CellType = gridType match {
    case GridType.UNIFORM => getCellInUniformGrid(data)
    case GridType.REGULAR => getCellInRegularGrid(data)
    case GridType.RECTANGULAR => getCellInRectangularGrid(data)
  }

}
