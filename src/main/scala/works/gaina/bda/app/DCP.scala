package works.gaina.bda.app

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import works.gaina.bda.spark.{CubeAnalyzer, DataFrameAnalyzer, DataFrameLoader, IO}
import works.gaina.bda.task.{ExApp, InfoPanel}
import works.gaina.bda.utils.{CSVStructureDetector, StrUtils}

/**
  * Created by gaina on 2/3/17.
  */
object DCP extends ExApp("DigitalCockpit") with DataFrameAnalyzer {

  override def process(df: DataFrame): Boolean = {
    //    val groupByAttributes = Array("Current Acquisition Step", "End-User Country")
    //    val outAttribute = "Unweighted Order Intake Total (€)"
    val result: Boolean = true
    val outFileName = "opp2.txt"
/*
    val xlsName = "Opportunity.xlsx"
    val df2 = IO.loadExcelSheet(df.sqlContext, pars.inpDirName, "Opportunity.xlsx",
      "Sheet1", true)
*/
    val rows = df.count
//    showInputSchema(df)
    df.rdd.foreach(println)
    println(s"Numer of rows in dataset $rows")
//    val nullByCols = countNulls(df)
//    nullByCols.foreach(println)
/*d
    println("==============DataFrame Types================")
    df.dtypes.foreach(println)
    println("==============DataFrame Types================")
*/
/*
    val data = df.rdd.take(rows)
    data.foreach(println)
    for (i<- 1 to rows) {
      println(s"$i > ")
      val row = data(i)
    }
*/
//    df.rdd.foreach(println)
//    analizeMissingValues(df.sqlContext.sparkContext, pars.fullFileName)
//    alignInptCSV(df.sqlContext.sparkContext, pars.fullFileName)
/*
    val sc = df.sqlContext.sparkContext
    val lines = CSVStructureDetector.groupRowsBySample(sc, pars.fullFileName)
    val rowsAfterCorrection = lines.length
    val msg = s"After grouping file contains $rowsAfterCorrection"
    InfoPanel.showInfo(msg)
    val rdd = sc.parallelize(lines)
    rdd.coalesce(1).saveAsTextFile(pars.inpDirName + "/" + outFileName)

     */

    //    val nullsByCols  = countNulls(df)
    //nullsByCols.foreach(println)
/*
    val groupByAttributes = Array("acando_cassidianregionidName", "Acando_CurrentAquisitionStep", "acando_WeightedExpectedRevenuesTotal")
    val outAttribute = "acando_WeightedExpectedRevenuesTotal"
    val data = DataFrameLoader.excludeRowsWithMissingValues(df.select(groupByAttributes(0), groupByAttributes(1), groupByAttributes(2)))
    val groups = CubeAnalyzer.applyToNonVoids(df, groupByAttributes)
    val groups = CubeAnalyzer.apply(df, groupByAttributes)
    showInputSchema(groups)
    groups.foreach(println)
     */
    result
  }

  def analizeMissingValues(sc: SparkContext, fileName: String): Unit = {
    val rdd = loadDataAsRdd(sc, fileName)
    val rows = rdd.count
    val rdd2 = rdd.map(StrUtils.countMatches(_, "\u0009"))
    val irrehular = rdd2.filter(_ != 441).count
    val irrehulaShare = (100 * irrehular / rows).toInt
//    rdd2.foreach(println)
    val lines = rdd.take(1000)
    for (s <- lines) {
      val k = StrUtils.countMatches(s, ";")
      if ((k != 441) && (k != 442))
        println(s"$k ==> $s")
    }
    println(s"Irregular lines => $irrehular from $rows - $irrehulaShare %")
  }

  def alignInptCSV(sc: SparkContext, fileName: String): Unit = {
//    val lines = CSVStructureDetector.getCSVasArray(sc, fileName)
    val lines = CSVStructureDetector.restoreStructure(sc, fileName)
    val rdd = sc.parallelize(lines)
    val rows = rdd.count
    val rdd2 = rdd.map(StrUtils.countMatches(_, ";"))
    val irrehular = rdd2.filter(_ != 441).count
    val irrehulaShare = (100 * irrehular / rows).toInt
    println(s"Irregular lines => $irrehular from $rows - $irrehulaShare %")
  }

  def saveInTextFile(lines: Array[String], outFileName: String): Boolean = {
    val result: Boolean = true
    result
  }

}
