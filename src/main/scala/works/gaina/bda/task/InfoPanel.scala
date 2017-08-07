package works.gaina.bda.task

import works.gaina.bda.utils.StrUtils

/**
  * Created by gaina on 12/20/16.
  */
object InfoPanel {

  val appInfoColor = Console.BLACK + Console.WHITE_B  + Console.BOLD
//  val infoColor = Console.WHITE
  val infoColor = Console.BLACK
  val warningColor = Console.GREEN
  val errorColor = Console.RED

  def makeMessage(msg: String, color: String) : String =
    StrUtils.colorize(StrUtils.enframeMessage(msg, StrUtils.TextGap(5, 1)), color)

  def showInfo(msg: String): Unit =
    println(makeMessage(msg, infoColor))

  def showWarning(msg: String): Unit =
    println(makeMessage(msg, warningColor))

  def showError(msg: String): Unit =
    println(makeMessage(msg, errorColor))

  def showAppName(appName: String): Unit = {
    val msg = appInfoColor + appName + Console.RESET
    println(makeMessage(msg, infoColor))
    //    val msg = "               " + appInfoColor + appName + Console.RESET
    //    println(msg)
  }

}
