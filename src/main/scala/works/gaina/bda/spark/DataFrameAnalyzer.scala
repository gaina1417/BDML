package works.gaina.bda.spark

import breeze.linalg.{DenseMatrix, DenseVector => BDV}
import org.apache.spark.sql.{DataFrame, Row}
import works.gaina.bda.struct.MatrixLinearAccess

import scala.collection.mutable.ArrayBuffer

trait DataFrameAnalyzer extends Serializable with MatrixLinearAccess {
  /**
    * Breeze Dense Vector of integer type
    */
  type CountsType = BDV[Int]
  /**
    * Breeze Dense Vector of double type
    */
  type StatsType = BDV[Double]
  /**
    * Tuple of two Breeze Dense Vector: first of integer type, second of double type
    */
  type CountsStatsType = (CountsType, StatsType)

  /**
    * Constructor of nullified result accumulator for counts.
    *
    * @param size - dimension of counts to be calculated
    * @return     - nullified vector of CountsType
    */
  def createNullCounts(size: Int): CountsType =
    BDV.zeros[Int](size)

  /**
    * Constructor of nullified result accumulator for stats.
    *
    * @param size - dimension of stats to be calculated
    * @return     - nullified vector of StatsType
    */
  def createNullStats(size: Int): StatsType =
    BDV.zeros[Double](size)

  /**
    * Constructor of nullified result accumulator for countsStats.
    *
    * @param countsSize - dimension of counts to be calculated
    * @param statsSize  - dimension of stats to be calculated
    * @return           - nullified vector of CountsStatsType
    */
  def createNullCountsStats(countsSize: Int, statsSize: Int): CountsStatsType =
    (createNullCounts(countsSize), createNullStats(statsSize))

  /**
    * Implements calculation of counts, defined in rowOperator
    *
    * @param df           - dataframe to be processed
    * @param rowOperator  - function, returns the counts for a row of dataframe
    * @return             - counts, calculated for all rows of dataframe
    */
  def perform(df: DataFrame, rowOperator: (Row) => CountsType): CountsType = {
    val row = df.head()
    val resultSize = rowOperator(row).length
    val nullValue: CountsType = createNullCounts(resultSize)
    df.rdd.aggregate(nullValue)(_ + rowOperator(_), _ + _)
  }

  /**
    *
    * @param result
    * @param row
    * @param rowOperator  - function, returns the stats for a row of dataframe
    * @param avoidAnyNull
    * @return
    */
  private def addStats(result: StatsType, row: Row, rowOperator: (Row) => StatsType, avoidAnyNull: Boolean): StatsType = {
    if (avoidAnyNull && row.anyNull)
      result
    else
      result + rowOperator(row)
  }

  /**
    * Implements calculation of stats, defined in rowOperator
    *
    * @param df           - dataframe to be processed
    * @param rowOperator  - function, returns the stats for a row of dataframe
    * @param avoidAnyNull - flag, which define ignore or not row, have at least one null value in row
    * @return             - stats, calculated for all rows of dataframe
    */
  def perform(df: DataFrame, rowOperator: (Row) => StatsType, avoidAnyNull: Boolean): StatsType = {
    val resultSize = rowOperator(df.head()).length
    val nullValue: StatsType = createNullStats(resultSize)
    df.rdd.aggregate(nullValue)(addStats(_, _, rowOperator, avoidAnyNull), _ + _)
  }

  /**
    * Сomponentwise summator of CountsStats tuple
    *
    * @param cs1  - first CountsStats tuple
    * @param cs2  - second CountsStats tuple
    * @return     - tuple as (sum of counts, sum of stats)
    */
  private def addCountsStats(cs1: CountsStatsType, cs2: CountsStatsType): CountsStatsType =
    (cs1._1 + cs2._1, cs1._2 + cs2._2)

  /**
    * Add to accumulator result of processing of given row
    *
    * @param result       - contains result of processing of previous rows
    * @param rowOperator  - function, returns the counts and stats for a row of dataframe
    * @param avoidAnyNull - flag, which define ignore or not row, have at least one null value in row
    * @return             - result of processing at the moment
    */
  private def addCountsStats(result: CountsStatsType, row: Row, rowOperator: (Row) => CountsStatsType, avoidAnyNull: Boolean): CountsStatsType = {
    if (avoidAnyNull && row.anyNull)
      result
    else
      addCountsStats(result, rowOperator(row))
  }

  /**
    * Implements calculation of counts and stats, defined in rowOperator
    *
    * @param df           - dataframe to be processed
    * @param rowOperator  - function, returns the counts and stats for a row of dataframe
    * @param avoidAnyNull - flag, which define ignore or not row, have at least one null value in row
    * @return             - counts and stats, calculated for all rows of dataframe
    */
  def perform(df: DataFrame, rowOperator: (Row) => CountsStatsType, avoidAnyNull: Boolean): CountsStatsType = {
    val res0 = rowOperator(df.head())
    val resultSize = (res0._1.length, res0._2.length)
    val nullValue: CountsStatsType = (BDV.zeros[Int](resultSize._1), BDV.zeros[Double](resultSize._2))
    df.rdd.aggregate(nullValue)(addCountsStats(_, _, rowOperator, avoidAnyNull), addCountsStats(_, _))
  }

  def nullsInRowOperator(row: Row): CountsType = {
    val fields = row.length
    val result = createNullCounts(fields)
    for (j <- 0 to fields - 1)
      if (row.isNullAt(j))
        result(j) = 1
    result
  }

  def voidsStringsInRowOperator(row: Row): CountsType = {
    val fields = row.length
    val result = createNullCounts(fields)
    for (j <- 0 to fields - 1)
      if (row.isNullAt(j))
        result(j) = 1
    result
  }

  def countNulls(df: DataFrame): CountsType =
    perform(df, nullsInRowOperator(_))

  def actualizeMinMax(currMinMax: StatsType, newValues: StatsType): StatsType = {
    val result = currMinMax.copy
    /*
        println("_____________________________________________")
        println(currMinMax)
        println(newValues)
        println("=============================================")
    */
    for (i <- 0 to currMinMax.length - 2 by 2) {
      if (result(i) > newValues(i))
        result(i) = newValues(i)
      if (result(i + 1) < newValues(i + 1))
        result(i + 1) = newValues(i + 1)
    }
    result
  }

  def getFromRowAndTwin(row: Row, cols: Array[Int]): StatsType = {
    val result = createNullStats(2 * cols.length)
    var l: Int = 0
    for (col <- cols) {
      val tmp = DataFrameLoader.getNumericalDataAsDouble(row, col)
      result(l) = tmp
      result(l + 1) = tmp
      l += 2
    }
    result
  }

  def renewMinMax(currMinMax: StatsType, row: Row, cols: Array[Int]): StatsType = {
    val data = getFromRowAndTwin(row, cols)
    val result = actualizeMinMax(currMinMax, data)
    result
  }

  def initMinMax(df: DataFrame, colsLength: Int): StatsType = {
    val result = createNullStats(2 * colsLength)
    for (i <- 0 to colsLength - 1) {
      result(2 * i) = Double.MaxValue
      result(2 * i + 1) = Double.MinValue
    }
    result
  }

  def getMinMax(df: DataFrame, cols: Array[Int]): StatsType = {
    val initValues = initMinMax(df, cols.length)
    val result = df.rdd.aggregate(initValues)(renewMinMax(_, _, cols), actualizeMinMax(_, _))
    result
  }

  def getMinMaxAsTouple(df: DataFrame, cols: Array[Int]): (BDV[Double], BDV[Double]) = {
    val minMax:BDV[Double] = getMinMax(df, cols)
    val min = new ArrayBuffer[Double]
    val max = new ArrayBuffer[Double]
    var l: Int = -1
    for (l <-0 to minMax.length - 2 by 2) {
      min.append(minMax(l))
      max.append(minMax(l + 1))
    }
    (new BDV(min.toArray), new BDV(max.toArray))
  }

  def getBinsValues(row: Row, cols: Array[Int], valMinMax: StatsType, bins: Int): CountsType = {
    val result = createNullCounts(cols.length)
    var l: Int = 0
    var bin: Int = -1
    for (j <- 0 to cols.length - 1) {
      val min = valMinMax(2 * l)
      val max = valMinMax(2 * l + 1)
      if (min < max) {
        val tmp = DataFrameLoader.getNumericalDataAsDouble(row, cols(j))
        if (tmp == max)
          bin = bins - 1
        else
          bin = ((tmp - min) / (max - min) * (bins -1)).toInt
        result(j) = bin
      }
      l += 1
    }
    result
  }

  def renewBins(countBins: CountsType, row: Row, cols: Array[Int], valMinMax: StatsType, bins: Int): CountsType = {
    val binsForRow = getBinsValues(row, cols, valMinMax, bins)
    val size = cols.length
    for (i <- 0 to size -1) {
      val bin = binsForRow(i)
      val l = linearIndexForNormalMatrix(bins, i, bin)
      countBins(l) += 1
    }
    countBins
  }

  def getMinMaxAndEDF(df: DataFrame, cols: Array[Int], bins: Int): (StatsType, CountsType) = {
    val initValues = createNullCounts(cols.length * bins)
    val valMinMax = getMinMax(df, cols)
    //    InfoPanel.showInfo("Extreme values for selected columns")
    println(valMinMax)
    val result = df.rdd.aggregate(initValues)(renewBins(_, _, cols, valMinMax, bins), _ + _)
    (valMinMax, result)
  }


  def getEDF(df: DataFrame, cols: Array[Int], bins: Int): CountsType = {
    getMinMaxAndEDF(df, cols, bins)._2
  }

  def getValuesTheirSqrAndPairwiseProductsResultSize(inpSize: Int): Int =
    2 * inpSize + sizeOfSymmetricMatrixWithoutDiagonal(inpSize)

  def getValuesTheirSqrAndPairwiseProductsFromRow(row: Row, numericalColumns: Array[Int]): BDV[Double] = {
    val inpSize = numericalColumns.length
    val size: Int = getValuesTheirSqrAndPairwiseProductsResultSize(inpSize)
    val result = createNullStats(size)
    var l: Int = 2 * inpSize - 1
    for (j1 <- 0 to numericalColumns.length - 1) {
      var ele1 = 0.0
      if (!row.isNullAt(numericalColumns(j1)))
        ele1 = DataFrameLoader.getNumericalDataAsDouble(row, numericalColumns(j1))
      //      val ele1 = row.getDouble(numericalColumns(j1))
      //      if (ele1 > 0.0)
      //        println(s"================$ele1==============")
      result(j1) = ele1
      result(j1 + inpSize) = math.pow(ele1, 2)
      for (j2 <- j1 + 1 to numericalColumns.length - 1) {
        //        val ele2 = row.getDouble(numericalColumns(j2))
        var ele2 = 0.0
        if (!row.isNullAt(numericalColumns(j2)))
          ele2 = DataFrameLoader.getNumericalDataAsDouble(row, numericalColumns(j2))
        l += 1
        result(l) = ele1 * ele2
      }
    }
    result
  }
/*
  def getCovarianceMatrix(df: DataFrame, cols: Array[Int], avoidAnyNull: Boolean): DenseMatrix[Double] = {
    val size = cols.length
  }
*/
}
