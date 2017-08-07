package works.gaina.bda.struct

import works.gaina.bda.common.Descriptors.{MatrixIndex, MatrixTypes}
import works.gaina.bda.common.Descriptors.MatrixTypes.MatrixTypes

/**
  * Created by gaina on 12/16/16.
  */
trait MatrixLinearAccess extends Serializable {

  /**
    * Calculate number of elements in matrix
    * @param rows - number of rows in matrix
    * @param cols - number of columns in matrix
    * @return     - number of elements in matrix
    */
  def sizeOfNormalMatrix(rows: Int, cols: Int) = rows * cols

  /**
    * Calculate number of elements of square matrix
    * @param cols - number of columns in matrix
    * @return     - number of elements in square matrix
    */
  def sizeOfSquareMatrix(cols: Int) = cols * cols

  /**
    * Calculate number of elements of symmetric matrix with diagonal elements
    * @param cols - number of columns in matrix
    * @return     - number of elements in symmetric matrix with diagonal
    */
  def sizeOfSymmetricMatrixWithDiagonal(cols: Int) = cols * (cols + 1) / 2

  /**
    * Calculate number of elements of symmetric matrix without diagonal elements
    * @param cols - number of columns in matrix
    * @return     - number of elements in symmetric matrix without diagonal
    */
  def sizeOfSymmetricMatrixWithoutDiagonal(cols: Int) = cols * (cols - 1) / 2


  /**
    * Calculate number of elements for different types of square matrix
    * @param matrixType - type of matrix
    * @param cols - number of columns in matrix
    * @return     - number of elements in square matrix
    */
  def sizeOfSquareMatrices(matrixType: MatrixTypes, cols: Int): Int = matrixType match {
    case MatrixTypes.SQUARE => sizeOfSquareMatrix(cols)
    case MatrixTypes.SYMMETRICWITHDIAGONAL => sizeOfSymmetricMatrixWithDiagonal(cols)
    case MatrixTypes.SYMMETRICWITHOUTDIAGONAL => sizeOfSymmetricMatrixWithoutDiagonal(cols)
  }

  /**
    * Calculate position of (row, col) matrix element in linear sequence
    * @param columns  - number of columns in matrix
    * @param row      - row of element
    * @param col      - column of element
    * @return         - position of (row, col) matrix element in linear sequence
    */
  def linearIndexForNormalMatrix(columns: Int, row: Int, col: Int): Int = row * columns + col

  /**
    * Calculate position of (row, col) element of symmetric matrix with diagonal in linear sequence
    * @param maxCols  - number of columns in matrix
    * @param row      - row of element
    * @param col      - column of element
    * @return         - position of (row, col) matrix element in linear sequence
    */
  def linearIndexForOfSymmetricMatrixWithDiagonal(maxCols: Int, row: Int, col: Int): Int ={
    val newrow = Math.min(row, col)
    val newcol = Math.max(row, col)
    linearIndexForNormalMatrix(maxCols, newrow, newcol) - ((newrow + 1) * newrow) / 2
  }

  /**
    * Calculate position of (row, col) element of symmetric matrix without diagonal in linear sequence
    * @param maxCols  - number of columns in matrix
    * @param row      - row of element
    * @param col      - column of element
    * @return         - position of (row, col) matrix element in linear sequence
    */
  def linearIndexForOfSymmetricMatrixWithoutDiagonal(maxCols: Int, row: Int, col: Int): Int = {
    val newrow = Math.min(row, col)
    val newcol = Math.max(row, col)
    linearIndexForNormalMatrix(maxCols, newrow, newcol) - ((newrow + 2) * (newrow + 1)) / 2
  }

  /**
    * Calculate position of (row, col) element for different types of matrix in linear sequence
    * @param maxCols  - number of columns in matrix
    * @param row      - row of element
    * @param col      - column of element
    * @return         - position of (row, col) matrix element in linear sequence
    */
  def getLinearIndex(matrixType: MatrixTypes, maxCols: Int, row: Int, col: Int): Int = matrixType match {
    case MatrixTypes.NORMAL => linearIndexForNormalMatrix(maxCols, row, col)
    case MatrixTypes.SYMMETRICWITHDIAGONAL => linearIndexForOfSymmetricMatrixWithDiagonal(maxCols, row, col)
    case MatrixTypes.SYMMETRICWITHOUTDIAGONAL => linearIndexForOfSymmetricMatrixWithoutDiagonal(maxCols, row, col)
  }

  /**
    * Calculate the row and col for linear index of element of normal matrix
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a tuple with respective (row, col)
    */
  def getRowColByLinearIndexForNormalMatrix(maxCols: Int, linearIndex: Int): (Int, Int) = {
    val row = linearIndex / maxCols
    val col = linearIndex - row * maxCols
    (row,col)
  }

  /**
    * Calculate the row and col for linear index of element of normal matrix
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a case class with respective (row, col)
    */
  def getMatrixIndexByLinearIndexForNormalMatrix(maxCols: Int, linearIndex: Int): MatrixIndex = {
    val rowCol = getRowColByLinearIndexForNormalMatrix(maxCols, linearIndex)
    MatrixIndex(rowCol._1, rowCol._2)
  }

  /**
    * Calculate the row and col for linear index of element of symmetric matrix with diagonal
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a tuple with respective (row, col)
    */
  def getRowColByLinearIndexForSymmetricMatrixWithDiagonal(maxCols: Int, linearIndex: Int): (Int, Int) = {
    val tmp = maxCols + 0.5
    val row = (tmp - math.sqrt (tmp * tmp - linearIndex)).toInt
    val col = linearIndex - row * maxCols
    (row,col)
  }

  /**
    * Calculate the row and col for linear index of element of symmetric matrix with diagonal
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a case class with respective (row, col)
    */
  def getMatrixIndexByLinearIndexForSymmetricMatrixWithDiagonal(maxCols: Int, linearIndex: Int): MatrixIndex = {
    val rowCol = getRowColByLinearIndexForSymmetricMatrixWithDiagonal(maxCols, linearIndex)
    MatrixIndex(rowCol._1, rowCol._2)
  }

  /**
    * Calculate the row and col for linear index of element of symmetric matrix without diagonal
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a tuple with respective (row, col)
    */
  def getRowColByLinearIndexForSymmetricMatrixWithoutDiagonal(maxCols: Int, linearIndex: Int): (Int, Int) = {
    val tmp = maxCols - 0.5
    val row = (tmp - math.sqrt (tmp * tmp - linearIndex)).toInt
    val col = linearIndex - row * maxCols + (row + 1) * (row + 2) / 2
    (row,col)
  }

  /**
    * Calculate the row and col for linear index of element of symmetric matrix without diagonal
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a case class with respective (row, col)
    */
  def getMatrixIndexByLinearIndexForSymmetricMatrixWithoutDiagonal(maxCols: Int, linearIndex: Int): MatrixIndex = {
    val rowCol = getRowColByLinearIndexForSymmetricMatrixWithoutDiagonal(maxCols, linearIndex)
    MatrixIndex(rowCol._1, rowCol._2)
  }

  /**
    * Calculate the row and col for linear index of element of different types of matrices
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a tuple with respective (row, col)
    */
  def getRowColByIndexForMatrix(matrixType: MatrixTypes, maxCols: Int, linearIndex: Int): (Int, Int) =  matrixType match {
    case MatrixTypes.NORMAL => getRowColByLinearIndexForNormalMatrix(maxCols, linearIndex)
    case MatrixTypes.SYMMETRICWITHDIAGONAL => getRowColByLinearIndexForSymmetricMatrixWithDiagonal(maxCols, linearIndex)
    case MatrixTypes.SYMMETRICWITHOUTDIAGONAL => getRowColByLinearIndexForSymmetricMatrixWithoutDiagonal(maxCols, linearIndex)
  }

  /**
    * Calculate the row and col for linear index of element of different types of matrices
    * @param maxCols      - number of columns in matrix
    * @param linearIndex  - position in linear sequence
    * @return             - a case class with respective (row, col)
    */
  def getMatrixIndexByLinearIndex(matrixType: MatrixTypes, maxCols: Int, linearIndex: Int): MatrixIndex = {
    val rowCol = getRowColByIndexForMatrix(matrixType, maxCols, linearIndex)
    MatrixIndex(rowCol._1, rowCol._2)
  }

}
