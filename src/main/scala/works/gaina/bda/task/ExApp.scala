package works.gaina.bda.task

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import works.gaina.bda.spark.{ConfigFactory, DataFrameLayout, IO, DataFrameLoader}
import works.gaina.bda.utils.StrUtils._

/**
  * Created by gaina on 12/20/16.
  */
abstract class ExApp(appName: String, var runsLocally: Boolean = false) extends Serializable {

  case class AppPars(inpDirName: String, fullFileName: String, outTableName: String, attributesNames: Array[String])

  var hostIP: String = ""
  var isTable: Boolean = false
  var pars = AppPars("", "", "", null)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      InfoPanel.showError(s"No arguments to start application $appName")
      System.exit(1)
    }
    val start = Calendar.getInstance().getTimeInMillis
    var modeMsg = "on Spark Cluster"
    pars = parseArguments(args)
    if (runsLocally)
      modeMsg = "locally"
    InfoPanel.showAppName(s"Task $appName started $modeMsg")
    val sc = createSparkContext
    val sqlContext = createSQLContext(sc)
//    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val s1 = pars.inpDirName
    val s2 = pars.fullFileName
    InfoPanel.showInfo(s"Start to load data from $s1 source $s2")
    val df = loadData(sqlContext, pars.fullFileName, isTable)
    val success = process(df)
    val elapsedTime = millisToStr(Calendar.getInstance().getTimeInMillis - start)
    if (success)
      InfoPanel.showInfo(s"Task finished - success, elapsed time $elapsedTime")
    else
      InfoPanel.showError("Task finished - failure")
  }

  def justifyTime(s: String) = {
    if (s.length == 1)
      "0" + s
    else
      s
  }

  def millisToStr(timeInMs: Long): String = {
    val timeInSec = timeInMs / 1000
    val hours = timeInSec / 3600
    val rest = timeInSec % 3600
    val mins = rest / 60
    val secs = rest % 60
    justifyTime(hours.toString) + ":" + justifyTime(mins.toString) + ":" + justifyTime(secs.toString)
  }

  def process(df: DataFrame): Boolean


  def createSparkContext: SparkContext = {
    if (runsLocally)
      new SparkContext(ConfigFactory.doLocal(driverHostIP = hostIP))
    else
      new SparkContext(ConfigFactory.applyWithoutPars(appName))
  }

  def createSQLContext(sc: SparkContext): SQLContext = {
    val spark = SparkSession.builder().getOrCreate()
    //SQLContext(sc)
    spark.sqlContext
/*
    if (runsLocally)
    else
      new org.apache.spark.sql.hive.HiveContext(sc)
     */
  }

  def isDirectory(dirName: String): Boolean = {
    if ((dirName != "") && (dirName.toUpperCase != "HIVE"))
      true
    else
      false
  }

  protected def parseArguments(args: Array[String]): AppPars = {
    val keywordLocal: String = "local"
    val parser = new ArgsParser(args)
    if (parser.isValidKey(keywordLocal)) {
      runsLocally = true
      val ip = parser.getValues(keywordLocal)(0)
      if (ip != "")
        hostIP = ip
    }
    val inpDir = parser.getValues("inpDir")(0)
    val fileName = parser.getValues("inpFile")(0)
    val outTableName: String = parser.getValues("outTable")(0)
    val attributesNames: Array[String] = parser.getValues("attrNames")
    isTable = !isDirectory(inpDir)
    if (isTable)
      AppPars(inpDir.toUpperCase.trim, fileName.trim, outTableName.trim, attributesNames)
    else {
      val dataSource = "FS"
      val fullFileName = inpDir.trim + "/" + fileName.trim
      AppPars(dataSource, fullFileName, outTableName.trim, attributesNames)
    }
  }
  def isParquetFile(fName: String): Boolean = {
    lastNCharsAsStr(fName,5) == ".parq"
  }

  def isCSVFile(fName: String): Boolean = {
    val fType = lastNCharsAsStr(fName,4)
    (fType == ".csv") || (fType == ".txt")
  }

  def isTSVFile(fName: String): Boolean = {
    lastNCharsAsStr(fName,4) == ".tsv"
  }

  def loadDataAsRdd(sc: SparkContext, fileName: String): RDD[String] = {
    sc.textFile(fileName)
  }

  protected def loadData(sqlContext: SQLContext, sourceName: String, isHiveTable: Boolean = false): DataFrame = {
    if (isHiveTable)
      IO.loadHiveTable(sqlContext, sourceName)
    else
    if (isParquetFile(sourceName))
      IO.loadParquetFile(sqlContext, sourceName)
    else
    if (isCSVFile(sourceName))
      IO.loadCSVFile(sqlContext, sourceName)
    else
      IO.loadTSVFile(sqlContext, sourceName)
  }

  def dataAsLabeledPoints(df: DataFrame, cols: Array[Int], colLabel: Int): RDD[LabeledPoint] =
    DataFrameLoader.dfToRDDOfLabeledPoints(df, cols, colLabel)

  def getTrainingAndTestSets(data: RDD[LabeledPoint],
                             weightOfTrainingSet: Double = 0.7, weightOfTestSet: Double = 0.3):
  (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val splits = data.randomSplit(Array(weightOfTrainingSet,  weightOfTestSet))
    (splits(0), splits(1))
  }

  def getTrainingAndTestSetsFromDataFrame(df: DataFrame, cols: Array[Int], colLabel: Int,
                                          weightOfTrainingSet: Double = 0.7, weightOfTestSet: Double = 0.3):
  (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val labeledPoints = DataFrameLoader.dfToRDDOfLabeledPoints(df, cols, colLabel)
    val splits = labeledPoints.randomSplit(Array(weightOfTrainingSet,  weightOfTestSet))
    (splits(0), splits(1))
  }

  def validNumericalColumns(df: DataFrame, attributesNames: Array[String]): Array[Int] = {
    if (attributesNames.length > 0)
      DataFrameLayout.getNumericalColumnsFromList(df: DataFrame, attributesNames, true)
    else
      DataFrameLayout.getNumericalColumns(df, true)
  }

  def showInfo(msg: String) = InfoPanel.showInfo(msg)

  def showWarning(msg: String) = InfoPanel.showWarning(msg)

  def showError(msg: String) = InfoPanel.showError(msg)

  def showInputSchema(df: DataFrame): Unit = {
    showInfo("Schema of input data")
    df.printSchema()
    val cols = df.columns.length
    println(s"Total number of columns $cols")
  }


}
