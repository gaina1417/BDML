package works.gaina.bda.app

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import works.gaina.bda.app.R3PI.{countryCars, getValidInfo}

object JourneyAnalysis {
  val dataDir = "/home/gaina/Data/Data Science Challenge/all journeys/"
  val fileName = "00DAC437-FF8B-4DA3-9E24-4EE1B1AA12EC.csv"
//  val carCountryFileName = dataDir + "carCountry"
  val locationAttributes = Array("lon", "lat")
  val stateAttributes = Array("eventType", "lon", "lat")
  val outfileName = "testR3PI.csv"
  val hostIP = "192.168.2.106"
  val master = "local[*]"
  val validCoordinates = Array((47.0, 9.0), (39.0, -9.0), (33.0, -97.0))
  val validCountries = Array("Switzerland", "Portugal", "TX, USA")

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession
    val src = loadData(spark, dataDir + fileName)
    val colX = src.columns.indexOf("x")
    val locations = src.filter(_.isNullAt(colX))
    val accelerations = src.filter(!_.isNullAt(colX))
    val locRows = locations.count
    val accRows = accelerations.count
    println(s"locations = $locRows")
    println(s"accelerationss = $accRows")
    describe(src)
    /*
    val df = getValidInfo(src)
    describe(df)
        val zones = roundCoordinates(df)
        stateCube(zones, locationAttributes)
        stateCube(zones, stateAttributes)
    countryCars(df)
    */
  }

  def createSparkSession: SparkSession =
    SparkSession.builder()
      .master(master)
      .appName("JourneyAnalysis")
      .config("spark.driver.host", hostIP)
      //      .enableHiveSupport()
      .getOrCreate()

  def showSparkConfig(session: SparkSession): Unit = {
    val cfgSet = session.sparkContext.getConf.getAll
    cfgSet.foreach(println)
  }

  def excludeRowsWithMissingValues(df: DataFrame): DataFrame =
    df.sqlContext.createDataFrame(df.rdd.filter(!_.anyNull), df.schema)

  def loadCSVFile(sqlContext: SQLContext, fullFileName: String, separator: String = ","): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", separator)
      .option("mode", "DROPMALFORMED")
      .option("nullValue", "")
      .load(fullFileName)

  def loadData(session: SparkSession, fullFileName:String, excludeMissing: Boolean = false): DataFrame = {
    val df = loadCSVFile(session.sqlContext, fullFileName)
    if (excludeMissing)
      excludeRowsWithMissingValues(df)
    else
      df
  }

  def saveData(df: DataFrame, fullFileName:String): Unit =
    df.write.csv(fullFileName)

  def describe(df: DataFrame): Unit = {
    df.printSchema()
    val rows = df.count
    println(rows)
  }

  def showData(df: DataFrame): Unit = {
    df.rdd.foreach(println)
  }

  def withoutFirst(strings: Array[String]): Array[String] =
    strings.slice(1, strings.length)


}
