package works.gaina.bda.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import works.gaina.bda.task.InfoPanel

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by gaina on 2/7/17.
  */
object CSVStructureDetector {

  val colsDelimiter = ";"
  val cols = 441

  case class NextFound(line: Int, betweenCount: Int)

  def rowIsComplete(cols: Int, separatorCount: Int): Boolean =
    (cols == separatorCount) || (cols == separatorCount + 1)

  def findNextValidRow(lines: Array[String], cols: Int, first: Int): NextFound = {
    var ind: Int = first - 1
    var countBetween: Int = 0
    var isComplete: Boolean = false
    do {
      ind += 1
      val k = StrUtils.countMatches(lines(ind), colsDelimiter)
      val isComplete = rowIsComplete(cols, k)
      if (!isComplete)
        countBetween += k
    } while (isComplete || (ind == lines.length - 1))
    if (ind == lines.length - 1)
      ind = -1
    NextFound(ind, countBetween)
  }

  def concatenateLines(lines: Array[String], l1: Int, l2: Int): String = {
    var s: String = ""
    for (l <- l1 to l2)
      s = s + " " + lines(l)
    s.trim
  }


  def groupRowsBySample(sc: SparkContext, fileName: String): Array[String] = {
    val result = new ArrayBuffer[String]
    val lines = getCSVasArray(sc, fileName)
    var l: Int = 0
    var quit: Boolean = false
    do {
      val next = findNextValidRow(lines, cols, l)
      if (next.line == -1)
        quit = true
      else {
        if ((next.betweenCount > 0) && (rowIsComplete(cols, next.betweenCount)))
          result.append(concatenateLines(lines, l, next.line-1))
        result.append(lines(next.line))
        l = next.line + 1
        if (l == lines.length -1)
          quit = true
      }
    } while (!quit)
    result.toArray
  }

  def restoreStructure(sc: SparkContext, fileName: String): Array[String] = {
    val result = new ArrayBuffer[String]
    val separator = "\n\r"
//    val lines = Source.fromFile(fileName).getLines
//    lines.foreach(println)
    val lines = getCSVasArray(sc, fileName)
    var i: Int = -1
    do {
      i += 1
      var s = lines(i)
      var k = StrUtils.countMatches(s, colsDelimiter)
      var isComplete = rowIsComplete(cols, k)
      if (!isComplete) {
        do {
          i += 1
          var s1 = lines(i)
          val k1 = StrUtils.countMatches(s, colsDelimiter)
          s = s + ' ' + s1
          k += k1
        } while ((k < cols) && (i < lines.length -1 ))
      }
      if (k == cols)
        result.append(s)
    } while (i < lines.length - 1)
    result.toArray
  }

  def getCSVasArray(sc: SparkContext, fileName: String): Array[String] = {
    val csv = sc.textFile(fileName)
    val lens = csv.map(StrUtils.countMatches(_, ";"))
    lens.foreach(println)
    val rows = csv.count().toInt
    InfoPanel.showInfo(s"Total rows in source file $rows")
    val lines = csv.take(rows)
    lines
  }

}
