package works.gaina.bda.app

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import works.gaina.bda.filters.SavitzkyGolayFilterOld
import works.gaina.bda.spark.DataFrameAnalyzer
import works.gaina.bda.stats.StatsConverter
import works.gaina.bda.task.{ExApp, InfoPanel}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 6/20/17.
  */

/* CONFIGURATION

--inpDir "/home/gaina/Data/LHT" --inpFile "drones.csv" --attrNames * --outTable dcp --local 0
--inpDir "/home/gaina/Data/LHT" --inpFile "GermanDrones.csv" --attrNames * --outTable dcp --local 0
--inpDir "/home/gaina/Data/LHT" --inpFile "GermanDronesSample.csv" --attrNames * --outTable dcp --local 0

 */

object Drones  extends ExApp("Drones") with DataFrameAnalyzer with StatsConverter {

  val dir1 = "/home/gaina/Data/LHT/GermanDrones"
  val suffix = " .csv"
  val provider1 = "GermanDrones"

  override def process(df: DataFrame): Boolean = {
    val result: Boolean = true
    showInputSchema(df)
    val x = df.rdd.map(_.getDouble(8)).collect()
//    val vcc =  SavitzkyGolayFilterOld.smooth((2,3),5,x)
//    vcc.foreach(println)
    result
  }
/*
SAVE NON-NULL ROWS
  override def process(df: DataFrame): Boolean = {
    val result: Boolean = true
    showInputSchema(df)
    val rdd = df.rdd.filter(!_.anyNull)
    val df2 = df.sqlContext.createDataFrame(rdd, df.schema)
    df2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/gaina/Data/LHT/dronesGDfull.csv")
    result
  }

  UNION ALL DATASETS FROM DIRECTORY
    override def process(df: DataFrame): Boolean = {
      val result: Boolean = true
      showInputSchema(df)
      //    val df2 = addData(df, dir1, fName1, provider1)
      val df2 = addDataFromDir(df, dir1, provider1)
      InfoPanel.showInfo("Final dataset contains " + df2.rdd.count.toString + " rows.")
      df2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/gaina/Data/LHT/dronesGD.csv")
      result
    }
  */

  def addDataFromDir(df: DataFrame, dir: String, provider: String): DataFrame = {
    val fileList = getListOfFilesNames(dir)
    var df1 = df
    for (l <- 0 to fileList.length -1) {
      val fullPath = fileList(l)
      val dsName = fullPath.replaceFirst(dir + "/", "").dropRight(suffix.length)
      val df2 = addData(df1, fileList(l), dsName,provider)
      df1 = df2
    }
    df1
  }

  def addData(df: DataFrame, fullPath: String, dsName: String, provider: String): DataFrame = {
    val rdd = loadData(df.sqlContext, fullPath).rdd.map(extendData(_, dsName,provider))
    val df2 = df.sqlContext.createDataFrame(rdd, df.schema)
    df.union(df2)
  }

  def getValueAsDouble(row: Row, col: Int): Double = {
    if ((col == 5) || (col == 7) || (col == 8))
      row.getInt(5).toDouble
    else
      row.getDouble(col)
  }

  def extendData(row: Row, fName: String, provider: String): Row = {
    val result = new ArrayBuffer[Any]
    var isHealth: Boolean = true
    if (fName(0).toUpper != 'H')
      isHealth = false
    for (j <- 0 to 8)
      if (row.isNullAt(j))
        result.append(null)
      else
        result.append(getValueAsDouble(row, j))
    result.append(isHealth)
    result.append(fName)
    result.append(provider.replace(' ','_'))
    Row(result(0), result(1), result(2),
        result(3), result(4), result(5),
        result(6), result(7), result(8),
        result(9), result(10), result(11)
    )
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFilesNames(dir: String): Array[String] = {
    val files = getListOfFiles(dir)
    files.map(_.getAbsolutePath).toArray
  }

}
