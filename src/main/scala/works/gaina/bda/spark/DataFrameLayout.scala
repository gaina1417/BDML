package works.gaina.bda.spark

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import works.gaina.bda.common.Descriptors
import works.gaina.bda.common.Descriptors.ArraysDifference
import works.gaina.bda.utils.ArrayScanner

/**
  * Created by gaina on 12/20/16.
  */
object DataFrameLayout extends Serializable {

  /** Separator of columns in description  */
  val colsSeparator = ","
  private val scanner = new ArrayScanner[String]

  /**
    *
    * @param df - Spark dataframe
    * @return   - types of columns from dataframe
    */
  def getColumnsDataType(df: DataFrame): Array[String] =
    df.dtypes.map(_._2)

  /**
    *
    * @param df             - dataframe
    * @param requestedType  - requested type in columns of dataframe
    * @return               - array with indexes of columns with requested data type
    */
  def indexesOfColumnsWithType(df: DataFrame, requestedType: String): Array[Int] =
    scanner.indexesWhere(getColumnsDataType(df), _ == requestedType)

  /** Returns numbers of columns with given DataType
    * @param df - DataFrame
    * @param colTypeAsStr - DataType as a String
    * */
  def getColumnsWithType(df: DataFrame, colTypeAsStr: String): Array[Int] = {
    val cols = df.columns.length
    val result = new ArrayBuffer[Int]
    for (j <- 0 to cols - 1) {
      if (df.dtypes(j)._2 == colTypeAsStr)
        result.append(j)
    }
    result.toArray
  }

  /** Returns names of columns with given DataType
    * @param df - DataFrame
    * @param colTypeAsStr - DataType as a String
    * */
  def getNamesOfColumnsWithType(df: DataFrame, colTypeAsStr: String): Array[String] = {
    val result = new ArrayBuffer[String]
    val colsList = getColumnsWithType(df, colTypeAsStr)
    if (colsList.length > 0)
      for(j <- colsList)
        result.append(df.columns(j))
    result.toArray
  }
/*
  def getColsAsStr(df: DataFrame, sparkTypes: SparkTypes): scala.collection.mutable.Map[String, String] = {
    val result = scala.collection.mutable.Map[String, String]()
    for (colTypeAsStr <- sparkTypes.ColumnsType) {
      val colsList = getColumnsWithType(df, colTypeAsStr)
      if (colsList.length > 0)
        result.update(colTypeAsStr, colsList.mkString(colsSeparator))
    }
    result
  }

  def getCols(df: DataFrame, sparkTypes: SparkTypes): scala.collection.mutable.Map[String, Array[Int]] = {
    val result = scala.collection.mutable.Map[String, Array[Int]]()
    for (colTypeAsStr <- sparkTypes.ColumnsType) {
      val colsList = getColumnsWithType(df, colTypeAsStr)
      if (colsList.length > 0)
        result.update(colTypeAsStr, colsList)
    }
    result
  }
*/
  /**   */
  def getDiffInStrings(strings1: Array[String], strings2: Array[String]): ArraysDifference = {
    val diff1 = strings1.diff(strings2)
    val diff2 = strings2.diff(strings1)
    ArraysDifference((diff1.length == 0) && (diff1.length == 0), diff1, diff2)
  }

  /** Returns differences in names of two dataframes
    * @param df1 - First DataFrame
    * @param df2 - Second DataFrame
    * */
  def getDiffByColumnsNames(df1: DataFrame, df2: DataFrame): ArraysDifference = {
    getDiffInStrings(df1.columns, df2.columns)
  }

  /** Returns differences in types of two dataframes
    * @param df1 - First DataFrame
    * @param df2 - Second DataFrame
    * */
  def getDiffByColumnsTypes(df1: DataFrame, df2: DataFrame): ArraysDifference =
    getDiffInStrings(df1.dtypes.map(_._2), df2.dtypes.map(_._2))

  /** Check if two data frames have the same structure  */
  def structuresAreTheSame(df1: DataFrame, df2: DataFrame): Boolean =
    getDiffByColumnsNames(df1, df2).areTheSame && getDiffByColumnsTypes(df1, df2).areTheSame

  /** Returns difference in structure of two dataframes
    * @param df1 - First DataFrame
    * @param df2 - Second DataFrame
    * Result is a tuple with first field - difference in names, secon field - difference in types of columns
    * */
  def getDiffInStructure(df1: DataFrame, df2: DataFrame): (ArraysDifference, ArraysDifference) =
    (getDiffByColumnsNames(df1, df2), getDiffByColumnsTypes(df1, df2))

  /** Show on the screen differences between 2 dataframes
    *
    * @param df1 - First DataFrame
    * @param df2 - Second DataFrame
    * @param title1 - alias for first dataframe
    * @param title2 - alias for second dataframe
    */
  def showDiffInNames(df1: DataFrame, df2: DataFrame, title1: String = "", title2: String = ""): Unit = {
    val diff = getDiffByColumnsNames(df1, df2)
    if (diff.areTheSame) {
      val fTitle = f"No Names differences between $title1 and $title2"
      println(
        s"""
           |=========================================================
           || No Names differences between $title1%10 and $title2%10      |
           |=========================================================
           |""".stripMargin)
    }
  }

  def getColumnsWithNames(df: DataFrame, columnsNames: Array[String]): Array[Int] = {
    val result = new ArrayBuffer[Int]
    var valid: Boolean = true
    for (name <- columnsNames) {
      val ind = df.columns.indexOf(name)
      if (ind != -1) {
        if (df.dtypes(ind)._2 != "DoubleType")
          valid = false
        else
          result.append(ind)
      }
      else
        valid = false
    }
    if (valid)
      result.toArray
    else
      null
  }

  def getTypesOfColumnsWithNames(df: DataFrame, columnsNames: Array[String]): Array[String] = {
    val result = new ArrayBuffer[String]
    val cols = getColumnsWithNames(df, columnsNames)
    if (cols != null) {
      for (col <- cols)
        result.append(df.dtypes(col)._2)
      result.toArray
    }
    else
      null
  }

  def getColumnsTypes(df: DataFrame): Array[String] =
    df.dtypes.map(_._2)

  def getColumnsWithTypes(df: DataFrame, dataTypes: Array[String]): Array[Int] = {
    val result = new ArrayBuffer[Int]
    val colTypes = getColumnsTypes(df)
    for (j <- 0 to colTypes.length - 1)
      if (dataTypes.contains(colTypes(j)))
        result.append(j)
    if (result.length > 0)
      result.toArray
    else
      null
  }

  def getValidColumns(df: DataFrame, columnsList: Array[String]): Array[String] =
    columnsList.filter(df.columns.contains(_))

  def getNumericalColumns(df: DataFrame, includeBoolean: Boolean = false): Array[Int] = {
    val numericalTypes = Array("BooleanType", "ByteType", "ShortType", "IntegerType", "LongType", "FloatType", "DoubleType")
    var beg = 1
    if (includeBoolean)
      beg = 0
    getColumnsWithTypes(df, numericalTypes.slice(beg, numericalTypes.length))
  }

  def getNumericalColumnsFromList(df: DataFrame, columnsList: Array[String], includeBoolean: Boolean = false): Array[Int] = {
    val numCols = getColumnsWithNames(df, columnsList)
    numCols.filter(getNumericalColumns(df, includeBoolean).contains(_))
  }


}
