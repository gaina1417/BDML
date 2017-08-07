package works.gaina.bda.app

import org.apache.spark.sql.{SQLContext, SparkSession}
import works.gaina.bda.spark.IO

/**
  * Created by gaina on 6/26/17.
  */
object TestRemote {

  def main(args: Array[String]): Unit = {
    val fullFileName = "/home/gaina/Data/FCPS/CSV/engytime.csv"
/*
    val hostIP = "127.0.0.1"
    val master = "local[2]"
 */
    val hostIP = "10.0.0.22"
    val master = "yarn"
    val tableName = "engytime"
//    val master = "spark://127.0.0.1:7077"
//    val master = "spark://192.168.8.100:7077"
    val spark =
      SparkSession.builder()
      .master(master)
      .appName("TestStandalone")
      .config("spark.driver.host", hostIP)
//      .enableHiveSupport()
      .getOrCreate()
//    val df = spark.table(tableName)
    val df = IO.loadCSVFile(spark.sqlContext: SQLContext, fullFileName)
    df.rdd.foreach(println)
    df.write.save(tableName)
    df.rdd.foreach(println)
    /*
    */
  }

}

/*
    spark.sql("create database test")
    val dbList = spark.sql("show databases")
   println(dbList)




  def main(args: Array[String]): Unit = {
//    val clusterIP = "192.168.56.103:7077"
    val clusterIP = "local[4]"
    val appName = "sparkInVM"
    val sparkConf = new SparkConf().setAppName(appName).setMaster(clusterIP)
    val sc = new SparkContext(sparkConf)
    val sqlContext = createSQLContext(sc)
    println(sc)
    println(sqlContext)
  }


 */
