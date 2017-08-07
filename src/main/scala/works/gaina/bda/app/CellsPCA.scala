package works.gaina.bda.app

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import works.gaina.bda.eda.{FrontDeskAgent, StructuredGrid}
import works.gaina.bda.spark.{DataFrameAnalyzer, DataFrameLoader}
import works.gaina.bda.stats.StatsConverter
import works.gaina.bda.task.ExApp

/**
  * Created by gaina on 2/27/17.
  */
/* CONFIGURATION
--inpDir "/home/gaina/Data/Iris" --inpFile "IrisWithHeader.csv" --attrNames * --outTable dcp --local 0
--inpDir "/home/gaina/Data/FCPS/CSV" --inpFile "engytime.csv" --attrNames * --outTable dcp --local 0
 */

object CellsPCA extends ExApp("CellsPCA") with DataFrameAnalyzer with StatsConverter {

  override def process(df: DataFrame): Boolean = {
    val result: Boolean = true
    val bins = 7
//    df.rdd.foreach(println)
    df.printSchema()
    // val cols = DataFrameLayout.indexesOfColumnsWithType(df, "DoubleType")
    //    val cols = Array(0,1,2,3)
    val cols = Array(0, 1)
    val rdd = df.rdd.map(DataFrameLoader.rowAsMLVector(_, cols))
//    rdd.foreach(println)
    val mtrx = new RowMatrix(rdd)
    val pc = mtrx.computePrincipalComponentsAndExplainedVariance(cols.length)
    println("================== PC in cols =====================")
    println(pc._1)
    println("================== Variance explained ====================")
    println(pc._2)
    val rddAll = test(df, cols, bins)
    result
  }

  def buildGrid(df: DataFrame, attributes: Array[Int], bins: Int): StructuredGrid = {
    val extremes = getMinMaxAsTouple(df, attributes)
    println("Extreme values")
    val dataSpaceBorders = enlargedBorders(extremes._1, extremes._2)
    val minValues = dataSpaceBorders._1
    val maxValues = dataSpaceBorders._2
    println(minValues)
    println(maxValues)
    val grid = new StructuredGrid(minValues, maxValues)
    grid.setGridUniform(bins)
    grid
  }

  def enlargedBorders(minValues: BDV[Double], maxValues: BDV[Double]): (BDV[Double], BDV[Double]) = {
    def enlargeFactor = 0.01
    val delta = (maxValues - minValues) :* enlargeFactor
    (minValues - delta, maxValues + delta)
  }

  def test(df: DataFrame, attributes: Array[Int], bins: Int): Unit = {
    val grid = buildGrid(df, attributes, bins)
    val agent = new FrontDeskAgent(df.rdd, attributes)
    agent.showAllPCA(grid)
  }

}