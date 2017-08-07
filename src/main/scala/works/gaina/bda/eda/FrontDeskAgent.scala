package works.gaina.bda.eda

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import works.gaina.bda.spark.DataFrameLoader
import breeze.linalg.{norm, DenseVector => BDV}
import breeze.numerics.abs
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 5/23/17.
  */
class FrontDeskAgent(rdd: RDD[Row], attributes: Array[Int]) extends Serializable {

  type CellType = BDV[Int]
  type PCAType = (Matrix, org.apache.spark.mllib.linalg.Vector)

  def keySeparator = "#"

  def getQuant(grid: StructuredGrid, row: Row, attributes: Array[Int]): CellType =
    grid.getCellInGrid(DataFrameLoader.rowAsBDV(row, attributes))

  def quantToStr(quant: CellType): String = {
    var s: String = ""
    for (q <- quant)
      s = s + keySeparator + q.toString
    s.substring(1)
  }

  def strKeyToQuant(sKey: String): CellType = {
    val data = sKey.split(keySeparator).map(_.toInt)
    new BDV(data)
  }

  def addQuant(grid: StructuredGrid, quants: Map[CellType, Int], row: Row): Map[CellType, Int] =
    quants + (grid.getCellInGrid(DataFrameLoader.rowAsBDV(row, attributes)) -> 1)

  def getCardinality(grid: StructuredGrid): RDD[(CellType, Int)] = {
    //    val bins = 40
    //  grid.setGridUniform(bins)
    val items = rdd.map(x => (quantToStr(getQuant(grid, x, attributes)), 1))
    //    items.foreach(println)
    var rows = items.count
    println(s"Items after initial mapping $rows")
    val result = items.reduceByKey(_ + _).sortBy(_._2, ascending = false)
    rows = result.count
    println(s"Items after reduceByKey $rows")
    // result.sortBy(_._2, ascending = false)
    result.map(q => (strKeyToQuant(q._1), q._2))
  }

  def distribute(grid: StructuredGrid): RDD[(String, Row)] = {
    val items = rdd.map(x => (quantToStr(getQuant(grid, x, attributes)), x))
    items.foreach(println)
    items
  }

  def getKeys(cells: RDD[(String, Row)]): RDD[String] =
    cells.map(_._1).distinct()

  def selectByKey(key: String, cells: RDD[(String, Row)]): RDD[Row] = {
    cells.filter(_._1 == key).map(_._2)
  }

  def getPCA(cells: RDD[(Row)]): PCAType = {
    val mtrx = new RowMatrix(cells.map(DataFrameLoader.rowAsMLVector(_, attributes)))
    mtrx.computePrincipalComponentsAndExplainedVariance(attributes.length)
  }

  def pcaByKey(key: String, cells: RDD[(String, Row)]): PCAType = {
    val src = selectByKey(key, cells)
    val mtrx = new RowMatrix(src.map(DataFrameLoader.rowAsMLVector(_, attributes)))
    mtrx.computePrincipalComponentsAndExplainedVariance(attributes.length)
  }

  def pcaByCells(grid: StructuredGrid): Array[(String, PCAType)] = {
    val result = new ArrayBuffer[(String, PCAType)]
    val cells = distribute(grid)
    cells.foreach(println)
    val keys = getKeys(cells).collect().sorted
    for (key <- keys) {
      val src = selectByKey(key, cells)
      if (src.count > 16) {
        val pca = getPCA(src)
        result.append((key, pca))
      }
    }
    result.toArray
  }

  def showAllPCA(grid: StructuredGrid): Unit = {
    val allPCA = pcaByCells(grid)
    getPairDistance(allPCA);
    for (pca <- allPCA) {
      val key = pca._1
      val pc = pca._2._1
      val ev = pca._2._2
      val s = key + " " + pc.toString().replace('\n',' ')
//      println(s)
/*
      println(s"================== PC in cols =====================$key")
      println(pc)
      println(s"================== Variance explained ====================$key")
      println(ev)
       */
    }
  }

  def evAsBDV(a:Array[Double]): (BDV[Double], BDV[Double]) = {
    val v1 = BDV(a(0),a(1))
    val v2 = BDV(a(2),a(3))
    (v1, v2)
  }

  def getCousineDist(v1:BDV[Double], v2:BDV[Double]): Double =
    v1 dot v2 / (norm(v1) * norm(v2))

  def getCousineDistBetweenEV(a:Array[Double], b :Array[Double]): (Double, Double) = {
    val av = evAsBDV(a)
    val bv = evAsBDV(b)
    (getCousineDist(av._1, bv._1), getCousineDist(av._2, bv._2))
  }

  def getPairDistance(pca: Array[(String, PCAType)]): Unit = {
    val maxThreshold = 0.2
    val keys = pca.map(_._1)
    val ev = pca.map(_._2._1)
    for (i <- 0 to keys.length-2)
      for  (j <- i+1 to keys.length-1) {
        val key1 = keys(i)
        val key2 = keys(j)
        val ev1 = ev(i).toArray
        val ev2 = ev(j).toArray
        val dist = getCousineDistBetweenEV(ev1, ev2)
        val d1 = dist._1
        val d2 = dist._2
        if (abs(d1) < maxThreshold)
          println(s"$key1 | $key2 | $d1 |$d2 ")
      }
  }

  /*

  def showAllPCA(grid: StructuredGrid): Unit = {
    val cells = distribute(grid)
    cells.foreach(println)
    val keys = getKeys(cells).collect().sorted
    for (key <- keys)
      pcaByKey(key, cells)
  }


  if (mtrx.numRows() > 1) {
    val pc = mtrx.computePrincipalComponentsAndExplainedVariance(attributes.length)
    println(s"================== PC in cols =====================$key")
    println(pc._1)
    println(s"================== Variance explained ====================$key")
    println(pc._2)
  }
  else
    println(s"Outlier Cell$key")
  pc

       */


}
