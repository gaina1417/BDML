package works.gaina.bda.spark

import org.apache.spark.SparkConf

/**
  * Created by gaina on 12/19/16.
  */
object ConfigFactory {

  val hostIP = "192.168.8.100"
//  val hostIP = "192.168.42.55"
//  val hostIP = "192.168.2.106"
  /** Create a configuration for Spark context, completed during start of job
    *
    * @param appName - name of application
    * @return a SparkConfiguration, which can be modified later
    */
  def applyWithoutPars(appName: String = "SparkApp"): SparkConf =
    new SparkConf().setAppName(appName)

  def doLocal(driverHostIP: String = "", appName: String = "LocalSparkApp", usesMaxCores: Boolean = false, cores: Int = 2): SparkConf = {
    var master = ""
    if (usesMaxCores)
      master = "local[*]"
    else
      master = "local[" + cores.toString + "]"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    if (driverHostIP != "")
      conf.set("spark.driver.host", hostIP)
    else
      conf
  }

}
