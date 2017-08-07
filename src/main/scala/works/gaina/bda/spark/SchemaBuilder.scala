package works.gaina.bda.spark

import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 12/20/16.
  */
object SchemaBuilder extends Serializable {

  val TypesHashTag = ","
  val ArrayHashTag = "[]"
  val RecordHashTag = "#"
  val MapHashTag = "=>"
  val NonNullableSign = "*"

  val typesTags = Array("NULL", "BOOL", "BYTE", "SHRT", "INT", "LONG",
    "FLT", "DBL", "DEC", "TS", "DATE", "STR",
    "BIN", "ARR", "MAP", "FLD", "REC")

  val primitiveTypesTags = typesTags.slice(0, 12)
  val compositeTypesTags = typesTags.slice(12, 16)

  def createArrayField(fieldName: String, arrayType: DataType, containsNull: Boolean=true, nullable: Boolean=true): StructField = {
    StructField(fieldName, ArrayType(arrayType, containsNull), nullable)
  }

  def createMapField(fieldName: String, keyType: DataType, valueType: DataType,
                     valueContainsNull: Boolean = true, nullable: Boolean = true): StructField = {
    StructField(fieldName, MapType(keyType, valueType, valueContainsNull), nullable)
  }

  def primitiveTypeByInd(typeInd: Int): DataType = typeInd match {
    case 0 => NullType
    case 1 => BooleanType
    case 2 => ByteType
    case 3 => ShortType
    case 4 => IntegerType
    case 5 => LongType
    case 6  => FloatType
    case 7  => DoubleType
    case 8  => TimestampType
    case 10  => StringType
    case 11  => BinaryType
  }

  def primitiveTypeByTag(typeTag: String): DataType = {
    var ind = typesTags.indexOf(typeTag)
    if (ind == -1)
      ind = 0
    primitiveTypeByInd(ind)
  }

  def validPrimitiveTypes(descriptor:String): Boolean = {
    var result = true
    val tags = descriptor.split(TypesHashTag)
    var i = 0
    do {
      if (primitiveTypesTags.indexOf(tags(i)) == -1)
        result = false
      i += 1
    } while (result && (i < tags.length))
    result
  }

  def mkPrimitiveTypesFrom(descriptor:String): Array[DataType] = {
    val result = new ArrayBuffer[DataType]
    if (validPrimitiveTypes(descriptor)) {
      val typeTags = descriptor.split(TypesHashTag)
      for (typeTag <- typeTags)
        result.append(primitiveTypeByTag(typeTag))
    }
    result.toArray
  }

  def mkColsNamesFrom(colsNumber: Int, columnsNames: String): Array[String] = {
    val result = new ArrayBuffer[String]
    val colNames = columnsNames.split(TypesHashTag)
    var colName: String = null
    val generateName = (colNames.length != colsNumber)
    for (j <- 0 to colsNumber - 1) {
      if (generateName)
        colName = "col" + (j + 1).toString
      else
        colName = colNames(j)
        colName = colNames(j)
      result.append(colName)
    }
    result.toArray
  }

  def mkNullableFrom(colsNumber: Int, nullableInfo: String): Array[Boolean] = {
    val result = new ArrayBuffer[Boolean]
    var nullability: Boolean = true
    val nullCols = nullableInfo.split(TypesHashTag)
    for (j <- 0 to colsNumber - 1) {
      if ((j < nullCols.length) && (nullCols(j) == NonNullableSign))
        nullability = false
      else
        nullability = true
      result.append(nullability)
    }
    result.toArray
  }

  def schemaValidDescriptors(typesDescriptor: String): Boolean = {
    val fieldsTypes = typesDescriptor.split(TypesHashTag)
    val colsTypes = primitiveTypesTags.intersect(fieldsTypes)
    colsTypes.sorted.sameElements(fieldsTypes.distinct.sorted)
  }

  def createSchema(typesDescriptor:String, columnsNames:String = "", nullableInfo: String = ""): StructType = {
    var result: StructType = null
    val fields = new ArrayBuffer[StructField]
    val colsTypes = mkPrimitiveTypesFrom(typesDescriptor)
    if (colsTypes != null) {
      val cols = colsTypes.length
      val colsNames = mkColsNamesFrom(cols, columnsNames)
      if (nullableInfo.length == 0)
        for (j <- 0 to cols - 1) {
          val structField = StructField(colsNames(j), colsTypes(j))
          fields.append(structField)
        }
      else {
        val colsNullability = mkNullableFrom(cols, nullableInfo)
        for (j <- 0 to cols - 1) {
          val structField = StructField(colsNames(j), colsTypes(j), colsNullability(j))
          fields.append(structField)
        }
      }
      result = StructType(fields)
    }
    result
  }

  def addToSchema(schema:StructType, typesDescriptor:String, columnsNames:String = "", nullableInfo: String = ""): StructType = {
    schema
  }

}
