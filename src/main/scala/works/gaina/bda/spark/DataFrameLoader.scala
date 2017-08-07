package works.gaina.bda.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 12/20/16.
  */
object DataFrameLoader extends Serializable {

  def excludeRowsWithMissingValues(df: DataFrame): DataFrame =
    df.sqlContext.createDataFrame(df.rdd.filter(!_.anyNull), df.schema)

  def selectRowsWithMissingValues(df: DataFrame): DataFrame =
    df.sqlContext.createDataFrame(df.rdd.filter(_.anyNull), df.schema)

  def rowContainsMissingValues(row: Row): Int = {
    if (row.anyNull)
      1
    else
      0
  }

  def getNullsRowsCount(df: DataFrame): Int =
    df.rdd.aggregate(0)(_ + rowContainsMissingValues(_) , _ + _)

  def getNonNullsRowsCount(df: DataFrame): Int =
    df.rdd.aggregate(0)(_ + 1 - rowContainsMissingValues(_) , _ + _)

  def getBooleanAsDouble(value: Boolean): Double = {
    if (value)
      1.0
    else
      0.0
  }

  def getNumericalDataAsDouble(row: Row, col: Int): Double = row.schema.fields(col).dataType match {
    case  DoubleType  => row.getDouble(col)
    case  FloatType => row.getFloat(col).toDouble
    case  IntegerType => row.getInt(col).toDouble
    case  LongType => row.getLong(col).toDouble
    case  ByteType => row.getByte(col).toDouble
    case  ShortType => row.getShort(col).toDouble
    case  BooleanType => getBooleanAsDouble(row.getBoolean(col))
  }

  def rowAsArrayOfDouble(row: Row, columns: Array[Int]): Array[Double] = {
    val result = new ArrayBuffer[Double]
    for (col <- columns)
      result.append(getNumericalDataAsDouble(row, col))
    result.toArray
  }

  def rowAsDoubleValues(row: Row, columns: Array[Int]): Row = {
    val result = new ArrayBuffer[Double]
    for (col <- columns)
      result.append(getNumericalDataAsDouble(row, col))
    Row(result)
  }

  def rowAsBDV(row: Row, columns: Array[Int]): BDV[Double] = {
    val result = BDV.zeros[Double](columns.size)
    var j: Int = -1
    for (col <- columns) {
      j += 1
      result(j) = getNumericalDataAsDouble(row, col)
    }
    result
  }

  def rowAsMLVector(row: Row, columns: Array[Int]): Vector  = {
    val data = rowAsArrayOfDouble(row, columns)
    Vectors.dense(data)
  }

  def rowAsLabeledPoint(row: Row, columns: Array[Int], colLabel: Int): LabeledPoint = {
    val v = rowAsMLVector(row, columns)
    val tmp = getNumericalDataAsDouble(row, colLabel)
    LabeledPoint(tmp, v)
  }

  def rowAsLabeledPoint(row: Row, columns: Array[Int]): LabeledPoint = {
    rowAsLabeledPoint(row, columns.dropRight(1), columns(columns.size - 1))
  }

  def getNumericalDataAsDouble(df: DataFrame, columns: Array[Int]): RDD[Row] =
    df.rdd.map(rowAsDoubleValues(_, columns))

  def dfToRDDOfLabeledPoints(df: DataFrame, columns: Array[Int]): RDD[LabeledPoint] = {
    df.rdd.map(rowAsLabeledPoint(_, columns))
  }

  def dfToRDDOfLabeledPoints(df: DataFrame, columns: Array[Int], colLabel: Int): RDD[LabeledPoint] = {
    df.rdd.map(rowAsLabeledPoint(_, columns, colLabel))
  }

}
