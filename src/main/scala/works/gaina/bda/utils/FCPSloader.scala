package works.gaina.bda.utils

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by gaina on 6/30/17.
  */
object FCPSloader extends App {

  def srcTag = ".lrn"
  def clsTag = ".cls"
  def remarkTag = '%'

  val dirFCPS = "/home/gaina/Data/FCPS/01FCPSdata"

  val fList = getFilesNamesFromDir(dirFCPS)
  val pairs = getPairsFromDir(fList)
  getData(dirFCPS + "/" + pairs(0)._1)
//  pairs.foreach(println)

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getFilesNamesFromDir(dirName: String): Array[String] = {
    val files = getListOfFiles(dirName)
    files.map(_.getName).toArray
  }

  def getPairsFromDir(filesNames: Array[String]): Array[(String, String)] = {
    val result = new ArrayBuffer[(String, String)]
    val srcNames = filesNames.filter(!_.endsWith(clsTag))
    val clsNames = filesNames.filter(!_.endsWith(srcTag))
    for (s <- srcNames) {
      val s1 = s.dropRight(4) + clsTag
      if (clsNames.contains(s1))
        result.append((s, s1))
    }
    result.toArray
  }

  def getData(fName: String): Unit = {
    val lines =  Source.fromFile(fName).getLines.filter(_.charAt(0) != remarkTag)
    val data = lines.map(_.split("\t"))
    for(d <- data) {
      println(d)
    }
  }



}
