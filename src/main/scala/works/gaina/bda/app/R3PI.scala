package works.gaina.bda.app

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by gaina on 7/13/17.
  */
/*
deviceId: string (nullable = true)
eventId: string (nullable = true)
eventTime: timestamp (nullable = true)
lat: double (nullable = true)
lon: double (nullable = true)
eventType: string (nullable = true)
*/

object R3PI {
  val dataDir = "/home/gaina/Data/R3PI/testDataset/"
  val fileName = "part-00000-711fabb0-5efc-4d83-afad-0e03a3156794.snappy.parquet"
  val carCountryFileName = dataDir + "carCountry"
  val locationAttributes = Array("lon", "lat")
  val stateAttributes = Array("eventType", "lon", "lat")
  val outfileName = "testR3PI.csv"
  val hostIP = "192.168.2.106"
  val master = "local[*]"
  val validCoordinates = Array((47.0, 9.0), (39.0, -9.0), (33.0, -97.0))
  val validCountries = Array("Switzerland", "Portugal", "TX, USA")

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession
    val src = loadData(spark, dataDir + fileName, true)
    val df = getValidInfo(src)
/*
    describe(df)
    val zones = roundCoordinates(df)
    stateCube(zones, locationAttributes)
    stateCube(zones, stateAttributes)
*/
    countryCars(df)
}

  def createSparkSession: SparkSession =
    SparkSession.builder()
      .master(master)
      .appName("TestStandalone")
      .config("spark.driver.host", hostIP)
      //      .enableHiveSupport()
      .getOrCreate()

  def showSparkConfig(session: SparkSession): Unit = {
    val cfgSet = session.sparkContext.getConf.getAll
    cfgSet.foreach(println)
  }

  def excludeRowsWithMissingValues(df: DataFrame): DataFrame =
    df.sqlContext.createDataFrame(df.rdd.filter(!_.anyNull), df.schema)

  def loadData(session: SparkSession, fullFileName:String, excludeMissing: Boolean = false): DataFrame = {
    val df = session.sqlContext.read.parquet(fullFileName)
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

  def getRoundCoordinates(row: Row): (Double, Double) = {
    val lat = math.round(row.getDouble(3)).toDouble
    val lon = math.round(row.getDouble(4)).toDouble
    (lat, lon)
  }

  def roundLonLat(row: Row): Row = {
    val dev = row.getString(0)
    val evnt = row.getString(1)
    val dts = row.getTimestamp(2)
    val lat = math.round(row.getDouble(3)).toDouble
    val lon = math.round(row.getDouble(4)).toDouble
    val evntType = row.getString(5)
    Row(dev, evnt, dts, lon, lat, evntType)
  }

  def roundCoordinates(df: DataFrame): DataFrame = {
    val rdd = df.rdd.map(roundLonLat(_))
    df.sqlContext.createDataFrame(rdd, df.schema)
  }

    def stateCube(df: DataFrame, attributesNames: Array[String], excludeDicesSlices: Boolean = true): Unit = {
    val cube = df.cube(attributesNames(0), withoutFirst(attributesNames): _*)
    cube.count.orderBy("count").filter(!_.anyNull).show()
  }

  def coordinatesIsValid(row: Row): Boolean = {
    val coord = getRoundCoordinates(row)
    val countryID = validCoordinates.indexOf(coord)
    countryID != -1
  }

  def carByCountry(row: Row): (String, String) = {
    val car = row.getString(0)
    val coord = getRoundCoordinates(row)
    val countryID = validCoordinates.indexOf(coord)
    if (countryID != -1) {
      val country = validCountries(countryID)
      (car, country)
    }
    else
      (null, null)
  }

  def carByCountryAsRow(row: Row): Row = {
    val cc = carByCountry(row)
    Row(cc._1,cc._2)
  }

  def countryCars(df: DataFrame): Unit = {
    val struct =
      StructType(
        StructField("car", StringType, true) ::
          StructField("country", StringType, true) :: Nil)
    val tmpRDD = (df.rdd.map(carByCountryAsRow(_))).filter(!_.anyNull)
    val rdd = (tmpRDD.map(x => (x.getString(1) -> x.getString(0)))).reduceByKey(_ + ";" + _)
    rdd.foreach(println)
  }

  def getValidInfo(df: DataFrame): DataFrame = {
    df.filter(coordinatesIsValid(_))
  }

}

/*
  def countryCars(df: DataFrame): Unit = {
    val struct =
      StructType(
        StructField("car", StringType, true) ::
          StructField("country", StringType, true) :: Nil)
    val rdd = (df.rdd.map(carByCountryAsRow(_))).filter(!_.anyNull)
    val df2 = df.sqlContext.createDataFrame(rdd, struct)
    val result = df2.select("car", "country").distinct()
    result.rdd.foreach(println)
    result.rdd.coalesce(1).saveAsTextFile(carCountryFileName)
  }
 */


