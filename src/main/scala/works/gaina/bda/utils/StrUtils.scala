package works.gaina.bda.utils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Exception.allCatch

/**
  * Created by gaina on 12/13/16.
  */
object StrUtils {

  val lineBreakTag = '|'
  val boxAbcSize = 6
  val boxHeavyABC = "━┃┏┓┗┛"
  // "\u2501" +   "\u2503" + "\u250F" + "\u2513" +  "\u2517"+ "\u251B"
  val boxDoubleABC = "═║╔╗╚╝"
  // "\u2550" +   "\u2551" + "\u2554" + "\u2557" +  "\u255A"+ "\u255D"
  val boxSingleABC = "─│┌┐└┘"
  // "\u2500" +   "\u2502" + "\u250C" + "\u2510" +  "\u2514"+ "\u2518"


  private val horizPos = 0
  private val vertPos = 1


  case class TextGap(horiz: Int, vert: Int)

  val intPattern = "[0-9]+".r
  val realPattern  = "[0-9]+[.][0-9]+".r

  def indexesOf(s: String, pattern: Char, from: Int = 0): Array[Int] = {
    val data = s.toCharArray.slice(from, s.length).zipWithIndex
    data.filter(_._1 == pattern).map(_._2 + from)
  }

  def stringToArray(s: String): Array[String] = {
    val lines = new ArrayBuffer[String]
    lines.append(s)
    lines.toArray
  }

  def isMultiline(s: String): Boolean =
    s.indexOf(lineBreakTag) != -1
  //    (s.indexOf(lineBreakTag) != -1) || s.indexOf(lineSeparator) != -1))


  def multiLineToArray(s: String): Array[String] = {
    val lines = new ArrayBuffer[String]
    if (isMultiline(s)) {
      val lineBreakInds = indexesOf(s + lineBreakTag, lineBreakTag)
      for (i <- 0 to lineBreakInds.length - 1)
        lines.append(s.substring(lineBreakInds(i) + 1, lineBreakInds(i + 1) - 1))
    }
    else
      lines.append(s)
    lines.toArray
  }


  def lastNCharsAsStr(s: String, n: Int): String = {
    s.substring(s.length-n, s.length)
  }

  def withoutFirst(strings: Array[String]): Array[String] =
    strings.slice(1, strings.length)

  def removeExtremesChars(s: String): String =
    s.substring(1, s.length - 1)

  def isIntType(s: String) = {

  }

  def canBeConvertedToDouble(s: String): Boolean =
    (allCatch opt s.toDouble).isDefined

  def currentTimestampAsString: String =
    (System.currentTimeMillis / 1000).toString

  def fillString(filler: Char, length: Int) =
    filler.toString * length

  def completeString(s: String, filler:Char, newLength: Int): String = {
    val diffLength = newLength - s.length
    if (diffLength > 0)
      s + fillString(filler, diffLength)
    else
      s
  }

  def posOfMatches(s: String, regex: String): Array[Int] = {
    val result = new ArrayBuffer[Int]
    val len = s.length
    val shift: Int = regex.length
    var i: Int = 0
    var pos: Int = -1
    do {
      pos = s.indexOf(regex, i)
      if (pos != -1) {
        result.append(pos)
        i = pos + shift
      }
    } while ((pos != -1) && (i < len))
    result.toArray
  }

  def countMatches(s: String, regex: String): Int =
    posOfMatches(s, regex).length

  def emptyLine(length: Int) =
    fillString(' ', length)

  def closedEmptyLine(length: Int, frameChar: Char = '*') =
    frameChar.toString + emptyLine(length - 2) + frameChar.toString

  def lineSeparator: String =
    sys.props("line.separator")

  def getGapLines(length: Int, verticalGap: Int = 1, frameChar: Char = '*') = {
    val lf = lineSeparator
    val cef = closedEmptyLine(length: Int, frameChar) + lf
    var result = cef
    for (l <- 1 to verticalGap - 1)
      result = result + cef
    result
  }

  def underlinedMessage(msg: String): String =
    Console.UNDERLINED + msg + Console.RESET

  def linesMaxLength(lines: Array[String]): Integer =
    lines.map(_.length).max

  def enframeMultiLines(msg: Array[String], gap: TextGap, frameABC: String = boxDoubleABC): String = {
    val shift = emptyLine(gap.horiz)
    val frameMsg = frameABC(1).toString + shift + msg(0) + shift + frameABC(1)
    val len = frameMsg.length
    val gapLines = getGapLines(len, gap.vert, frameABC(1))
    val topLine = frameABC(2).toString + fillString(frameABC(0), len-2) + frameABC(3)
    val bottomLine = frameABC(4).toString + fillString(frameABC(0), len-2) + frameABC(5)
    Console.RESET + topLine + lineSeparator + gapLines + frameMsg + lineSeparator + gapLines + bottomLine
  }

  def enframeMessage(msg: String, gap: TextGap, frameABC: String = boxSingleABC): String = {
    enframeMultiLines(stringToArray(msg), gap, frameABC)
  }

  def enframeMessageWithChar(msg: String, gap: TextGap, frameChar: Char = '*'): String =
    enframeMessage(msg, gap, fillString(frameChar, boxAbcSize))

  def colorize(s: String, color: String): String =
    color + s + Console.RESET

  def strXOR(s1: String, s2: String): Array[Int] = {
    val result = new ArrayBuffer[Int]
    var minLen = s1.length
    if (minLen > s2.length)
      minLen = s2.length
    for (i <- 0 to minLen - 1)
      result.append(s1(i) ^ s2(i))
    result.toArray
  }

  def firstMatches(s1: String, s2: String): Int =
    strXOR(s1, s2).indexWhere(_ != 0)

}
