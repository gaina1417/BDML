package works.gaina.bda.spark

import org.apache.spark.SparkContext

/**
  * Created by gaina on 12/19/16.
  */
class HDFSUtils(sc: SparkContext) extends Serializable {
  val conf = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)


  def getFullFileName(pathName: String, fileName: String) =
    pathName + "/" + fileName

  def fileExists(fullFileName: String): Boolean = {
    fs.exists(new org.apache.hadoop.fs.Path(fullFileName))
  }

  def fileExists(pathName: String, fileName: String): Boolean = {
    fs.exists(new org.apache.hadoop.fs.Path(getFullFileName(pathName, fileName)))
  }

}
