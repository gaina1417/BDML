package works.gaina.bda.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 2/3/17.
  */
object CubeAnalyzer extends Serializable {

  val countFieldName = "count"
  val countFieldType = LongType
  val countNullable = false
  val countFieldTypeAsString = "LongType"
  val likeSQLParam: Boolean = true

  /**  */
  def withoutFirst(strings: Array[String]): Array[String] =
    strings.slice(1, strings.length)

  /**  Create a array with numbers of columns with same type of data */
  def getColumnsWithType(df: DataFrame, colTypeAsStr: String): Array[Int] = {
    val cols = df.columns.length
    val result = new ArrayBuffer[Int]
    for (j <- 0 to cols - 1) {
      if (df.dtypes(j)._2 == colTypeAsStr)
        result.append(j)
    }
    result.toArray
  }

  /** Create a dataframe, containing the values of given attributes and frequency (count)
    * of this combination in data */
  def apply(df: DataFrame, attributesNames: Array[String]):  DataFrame = {
    val cube = df.cube(attributesNames(0), withoutFirst(attributesNames): _*)
    cube.count.orderBy("count")
    //    cube.count.orderBy(desc("count"))
  }

  /**  Create a RDD of Row, containing the values of given attributes and frequency (count) of this
    * combination in data. Rows, which don't contain null values in all atrributes are considered only */
  def applyToNonNulls(df: DataFrame, attributesNames: Array[String]):  RDD[Row] = {
    val allData = apply(df, attributesNames)
    val lastColToFilter = attributesNames.length - 1
    allData.rdd.filter(!DFUtils.containsNulls(_, 0, lastColToFilter))
  }

  def applyToNonNulls(df: DataFrame, attributesNames: Array[String], outTableName: String): DataFrame  = {
    val rdd = applyToNonNulls(df, attributesNames)
    val schema = getCubeSchemaFor(DFUtils.getSchemaFor(df,attributesNames))
    val outDF = IO.dfFromRDD(df.sqlContext, rdd, schema)
    outDF.write.saveAsTable(outTableName)
    outDF
  }

  def getNulls(df: DataFrame, attributesNames: Array[String]):  RDD[Row] = {
    val allData = apply(df, attributesNames)
    val lastColToFilter = attributesNames.length - 1
    allData.rdd.filter(DFUtils.containsNulls(_, 0, lastColToFilter))
  }

  /**  Create a RDD of Row, containing the values of given attributes with StringType,
    * and frequency (count) of this combination in data. Rows, which don't contain empty values
    * in all atrributes are considered only */
  def applyToNonVoids(df: DataFrame, attributesNames: Array[String]):  RDD[Row] = {
    val allData = apply(df, attributesNames)
    val colsList = getColumnsWithType(allData, "StringType")
    allData.rdd.filter(!DFUtils.containsVoids(_, colsList))
  }

  def applyToNonVoids(df: DataFrame, attributesNames: Array[String], outTableName: String): DataFrame  = {
    val rdd = applyToNonVoids(df, attributesNames)
    val schema = getCubeSchemaFor(DFUtils.getSchemaFor(df,attributesNames))
    val outDF = IO.dfFromRDD(df.sqlContext, rdd, schema)
    outDF.write.saveAsTable(outTableName)
    outDF
  }

  /**  Create a RDD of Row, containing the values of given attributes and frequency (count) of
    * this combination in data. Rows, which don't contain null values in all atrributes and
    * don't contain empty values in StringType attributes are considered only */
  def applyToNonMissing(df: DataFrame, attributesNames: Array[String]):  RDD[Row] = {
    val allData = apply(df, attributesNames)
    val lastColToFilter = attributesNames.length - 1
    val colsList = getColumnsWithType(allData, "StringType")
    allData.rdd.filter(DFUtils.containsMissing(_, 0, lastColToFilter, colsList))
  }

  def applyToNonMissing(df: DataFrame, attributesNames: Array[String], outTableName: String): DataFrame  = {
    val rdd = applyToNonMissing(df, attributesNames)
    val schema = getCubeSchemaFor(DFUtils.getSchemaFor(df,attributesNames))
    val outDF = IO.dfFromRDD(df.sqlContext, rdd, schema)
    outDF.write.saveAsTable(outTableName)
    outDF
  }

  /** Check, if DataFrame is result of application of OLAP cube - the last column must have
    * "count" as name and "Long" as type of field */
  def isCubeResults(df: DataFrame): Boolean = {
    val lastColType = df.dtypes(df.dtypes.length-1)
    (lastColType._1 == countFieldName) && (lastColType._2 == countFieldTypeAsString)
  }

  /** Filter the Cube RDD by threshold, applied to count */
  def filterByQuote(data: RDD[Row], threshold: Double): RDD[Row] = {
    val rowsCount = data.count
    val lowLimit = (rowsCount * threshold).toLong
    val countCol = data.take(1)(0).schema.fields.length - 1
    data.filter(_.getLong(countCol) > lowLimit)
  }

  def getCubeSchemaFor(schema: StructType): StructType = {
    DFUtils.addToSchema(schema, new StructField(countFieldName, countFieldType, countNullable))
  }

  def getCubeSchemaFor(df: DataFrame): StructType = {
    getCubeSchemaFor(df.schema)
  }

  /** Show the contain of Cube Dataframe of all data */
  def showResults(df: DataFrame): Unit = {
    val len = df.rdd.count.toInt
    val rows = df.rdd.take(len).reverse
    println("================================================")
    rows.foreach(println)
    println("================================================")
  }

  /** Show the contain of Cube RDD for transformed data */
  def showFilteredResults(rdd: RDD[Row]): Unit = {
    val len = rdd.count.toInt
    val rows = rdd.take(len).reverse
    println("================================================")
    rows.foreach(println)
    println("================================================")
  }


}
