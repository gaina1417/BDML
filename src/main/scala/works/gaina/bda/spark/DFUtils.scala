package works.gaina.bda.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 2/3/17.
  */
object DFUtils extends Serializable {

  def containsNulls(row: Row, colBeg: Int, colEnd: Int): Boolean = {
    var result = false
    var col = colBeg
    do {
      if (row.isNullAt(col))
        result = true
      col += 1
    } while (!result && (col <= colEnd))
    result
  }

  def containsVoids(row: Row, stringColsList: Array[Int]):Boolean = {
    var result = false
    for (col <- stringColsList)
      if (!row.isNullAt(col))
        if (row.getString(col).length == 0)
          result = true
    result
  }

  def containsMissing(row: Row, colBeg: Int, colEnd: Int, stringColsList: Array[Int]): Boolean = {
    var result = false
    var col = colBeg
    do {
      if (row.isNullAt(col))
        result = true
      else {
        if (stringColsList.indexOf(col) != -1)
          if (row.getString(col).length == 0)
            result = true
        col += 1
      }
    } while (!result && (col <= colEnd))
    result
  }

  def validAttributes(df: DataFrame, attrNames: Array[String]): Boolean = {
    //    containsAnother(df.columns, attrNames)
    val colNames = df.columns.intersect(attrNames).sorted
    colNames.sameElements(attrNames.sorted)
  }

  def getSchemaFor(df: DataFrame, columnsNames: Array[String]): StructType = {
    val result = new ArrayBuffer[StructField]
    for (j <- 0 to columnsNames.length - 1) {
      val l = df.columns.indexOf(columnsNames(j))
      if (l != -1)
        result.append(df.schema.fields(l))
    }
    if (result.length > 0)
      StructType(result.toArray)
    else
      null
  }

  def addToSchema(schema: StructType, newField: StructField): StructType = {
    val result = new ArrayBuffer[StructField]
    for (fld <- schema)
      result.append(fld)
    result.append(newField)
    StructType(result.toArray)
  }

}
