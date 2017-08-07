package works.gaina.bda.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import works.gaina.bda.utils.StrUtils

/**
  * Created by gaina on 12/20/16.
  */
object IO extends Serializable {

  val HorizTab = "\u0009"
  val semiColon = ";"

  def loadParquetFile(sqlContext: SQLContext, fullFileName: String): DataFrame =
    sqlContext.read.parquet(fullFileName)

  def loadHiveTable(sqlContext: SQLContext, tableName: String): DataFrame =
    sqlContext.sql(s"""SELECT * FROM $tableName""")

  /**
    * Create Dataframe from RDD, using given schema
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param schema - SparkSQL schema, describes rdd
    * @return - dataframe, containing rdd
    */

  def dfFromRDD(sqlContext: SQLContext, rdd: RDD[Row], schema: StructType): DataFrame = {
    sqlContext.createDataFrame(rdd, schema)
  }

  /**
    * Create Dataframe from RDD, using given schema's description
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param typesDescriptor - string descriptor of types of columns
    * @param columnsNames - string descriptor of names of columns
    * @param nullableInfo - string descriptor of nullability of columns
    * @return
    */

  def dfFromRDD(sqlContext: SQLContext, rdd: RDD[Row], typesDescriptor:String,
                columnsNames:String = "", nullableInfo: String = ""): DataFrame = {
    val schema = SchemaBuilder.createSchema(typesDescriptor, columnsNames, nullableInfo)
      if (schema != null)
        dfFromRDD(sqlContext, rdd, schema)
      else
    null
  }

  /**
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param schema - SparkSQL schema, describes rdd
    * @param outFileName - name of output paquet file
    * @return boolean result of save success / failure
    */

  def saveRDDToParquet(sqlContext: SQLContext, rdd: RDD[Row], schema: StructType, outFileName: String): Boolean = {
    val saved: Boolean = true
    dfFromRDD(sqlContext, rdd, schema).write.parquet(outFileName)
    saved
  }

  /**
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param typesDescriptor - string descriptor of types of columns
    * @param columnsNames - string descriptor of names of columns
    * @param nullableInfo - string descriptor of nullability of columns
    * @param outFileName - name of output parquet file
    * @return boolean result of save success / failure
    */

  def saveRDDToParquet(sqlContext: SQLContext, rdd: RDD[Row], typesDescriptor:String,
                       columnsNames:String = "", nullableInfo: String = "", outFileName: String): Boolean = {
    var saved: Boolean = false
    val df = dfFromRDD(sqlContext, rdd, typesDescriptor, columnsNames, nullableInfo)
    if (df != null) {
      df.write.parquet(outFileName)
      saved = true
    }
    saved
  }

  /**
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param schema - SparkSQL schema, describes rdd
    * @param tableName - name of output Hive table
    * @return boolean result of save success / failure
    */
  def saveRDDToHive(sqlContext: SQLContext, rdd: RDD[Row], schema: StructType, tableName: String): Boolean = {
    val saved: Boolean = true
    dfFromRDD(sqlContext, rdd, schema).write.saveAsTable(tableName)
    saved
  }

  /**
    *
    * @param sqlContext - hiveContext of application
    * @param rdd - work RDD
    * @param typesDescriptor - string descriptor of types of columns
    * @param columnsNames - string descriptor of names of columns
    * @param nullableInfo - string descriptor of nullability of columns
    * @param outFileName - name of output paquet file
    * @return boolean result of save success / failure
    */
  def saveRDDToHive(sqlContext: SQLContext, rdd: RDD[Row], typesDescriptor:String,
                    columnsNames:String = "", nullableInfo: String = "", outFileName: String): Boolean = {
    var saved: Boolean = false
    val df = dfFromRDD(sqlContext, rdd, typesDescriptor, columnsNames, nullableInfo)
    if (df != null) {
      df.write.saveAsTable(outFileName)
      saved = true
    }
    saved
  }

  def getTableRows(sqlContext: SQLContext, tableName: String): Long = {
    val df = sqlContext.sql(s"""SELECT * FROM $tableName""")
    df.count
  }

  def isTableNonEmpty(sqlContext: SQLContext, tableName: String): Boolean =
    getTableRows(sqlContext, tableName) > 0

  def getColumnsListFromTable(sqlContext: SQLContext, tableName: String, likeSQLParam: Boolean = false): String = {
    var separator = ","
    if (likeSQLParam)
      separator = ", $"
    val df = sqlContext.sql(s"""SELECT * FROM $tableName""")
    var result = df.columns.mkString(separator)
    if (likeSQLParam)
      result = "$" + result
    result
  }

  def createTempTableName(root: String): String =
    root + StrUtils.currentTimestampAsString

  def generateOutAttributeName(attributeName: String, endPhrase: String): String =
    attributeName + endPhrase

  def getFullAttributeName(tableName: String, attributeName: String): String =
    tableName + "." + attributeName

  def dropTable(sqlContext: SQLContext, tableName: String): Unit =
    sqlContext.sql(s"""DROP TABLE $tableName""")

  def renameTable(sqlContext: SQLContext, tableName: String, newTableName: String): Unit = {
    sqlContext.sql(s"""ALTER TABLE $tableName RENAME TO $newTableName""")
  }

  def loadCSVFile(sqlContext: SQLContext, fullFileName: String, separator: String = ","): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", separator)
      .option("mode", "DROPMALFORMED")
      .option("nullValue", "")
      .load(fullFileName)

  def loadTSVFile(sqlContext: SQLContext, fullFileName: String): DataFrame =
    loadCSVFile(sqlContext, fullFileName, separator = HorizTab)

  def loadASVFile(sqlContext: SQLContext, fullFileName: String, newSeparator: String): DataFrame =
    loadCSVFile(sqlContext, fullFileName, semiColon)

  def loadExcelSheet(sqlContext: SQLContext, pathToDir: String, fileName: String,
                     sheetName: String, useHeader: Boolean): DataFrame = {
    var strUseHeader: String = "true"
    if (!useHeader)
      strUseHeader = "false"
    val df = sqlContext.read
      .format("com.crealytics.spark.excel")
      .option(pathToDir, fileName)
      .option("sheetName", sheetName)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", strUseHeader)
      .load()
    df
  }

}
