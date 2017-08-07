package works.gaina.bda.app

import breeze.numerics.pow
import vegas._
// import vegas.sparkExt._
 import vegas.render.WindowRenderer._

/**
  * Created by gaina on 3/1/17.
  */
object SandBox extends App {

  override def main(args: Array[String]) = {
    implicit val renderer = vegas.render.ShowHTML(str => println(s"The HTML is $str"))
//    plotVegas
  }

  def binsLoop: Unit = {
    var N: Double = 1E9
    var m: Int = 30
    var l: Int = 0
    do {
      l = bins(N, m)
      m += 1
    } while (l > 2)
  }

  def bins(points: Double, dim: Int): Int = {
    val result = (math.round(pow(points, 1 / dim.toDouble))).toInt
    println("-----------------------------------------")
    println(s"Points $points, dimensions $dim, bins $result")
    println
    result
  }

  def strCheckSum(s: String, outputEnabled: Boolean) = {
    val sUp = s.toUpperCase()
    val pivot = 'A'.toInt
    var result: Int = 0
    for (ch <- sUp)
      result += ch.toInt - pivot + 1
    if (outputEnabled)
      println(s"$s => $result")
    result
  }

  def showCheckSums: Unit = {
    strCheckSum("Multi-Level Association/Dissociation", true)
    strCheckSum("Multi-LevelAssociationDissociation", true)
    strCheckSum("MultiLevelAssociationDissociation", true)
  }

/*

  def scatterPlot: Unit = {
    Vegas("Sample Scatterplot", width=800, height=600)
      .withURL(Cars)
      .mark(Point)
      .encodeX("Horsepower", Quantitative)
      .encodeY("Miles_per_Gallon", Quantitative)
      .encodeColor(field="Acceleration", dataType=Quantitative, bin=Bin(maxbins=5.0))
      .show
  }


  def plotVegas: Unit = {

    val plot = Vegas("Country Pop").
      withData(
        Seq(
          Map("country" -> "USA", "population" -> 314),
          Map("country" -> "UK", "population" -> 64),
          Map("country" -> "DK", "population" -> 80)
        )
      ).
      encodeX("country", Nom).
      encodeY("population", Quant).
      mark(Bar)
    plot.show
  }

  def renderHTML = {
    println(plot.html.pageHTML) // a complete HTML page containing the plot
    println(plot.html.frameHTML("foo")) // an iframe containing the plot
 */

}
