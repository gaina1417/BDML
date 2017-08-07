package works.gaina.bda.app

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import works.gaina.bda.spark.CubeAnalyzer
import works.gaina.bda.task.ExApp
import works.gaina.bda.utils.StrUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 5/15/17.
  *
  */
object FPG extends ExApp("FP-Growth") {
// --inpDir "/home/gaina/Data/FrequentPatternMining" --inpFile "mushroom.csv" --attrNames * --outTable dcp --local 0
// --inpDir "/home/gaina/Data/ABWH/1" --inpFile "Verbrauch.csv" --attrNames * --outTable dcp --local 0
  val testFlag: Boolean = false
  val dateAttr = "B_DATUM"
  var itemAttr = "HTZ"
  val minSupport = 0.5
  val numPartitions = 8
  val minConfidence = 0.95
  val maxItems = 200

  override protected def loadData(sqlContext: SQLContext, sourceName: String, isHiveTable: Boolean): DataFrame = {
    val df = super.loadData(sqlContext, sourceName, isHiveTable).select(dateAttr, itemAttr).groupBy(dateAttr, itemAttr).count()
    val minPresence = minSupport * df.count.toInt
//    val df2 = df
    val df2 = df.filter("count > 10").orderBy("count").select(dateAttr, itemAttr)
    df2
  }

  override def process(df: DataFrame): Boolean = {
    val result: Boolean = true
//    df.rdd.foreach(println)
    df.printSchema()
    val minItems = 0.005 * df.count
//    val attributes = Array("f5","f9","f14","f15","f17");
    val attributes = Array("f1","f2","f3","f4","f5","f6","f7","f8","f9","f10",
                           "f11","f12","f13","f14","f15","f16","f17","f18","f19","f20","f21","f22","f23");
/*
    val attributes = Array("MANDANT", "HTZ", "BEN", "QUELLE_ORT", "ZIEL_ORT",
      "ORG", "KST", "LKS", "TAG", "STK", "ME_TXT", "B_DATUM");
*/
    if (testFlag) {
      val data = df.rdd.map(rowToArrayOfString(_))
      FPGrowth(data)
    }
    else {
      val dataByDay = df.rdd.map(rowToMap(_)).reduceByKey(_ + "," + _)
      val data = dataByDay.map(_._2.split(",").distinct.reverse.slice(0,maxItems))
      FPGrowth(data)
    }

    result
  }

  def rowToMap(row: Row): (String, String) = {
    val bdate = row.getString(0)
    val item = row.getString(1)
    (bdate , item)
  }

  def rowToArrayOfString(row:Row): Array[String] = {
    val result = new ArrayBuffer[String]
    for (j <- 0 to row.length-1)
      result.append(row.getInt(j).toString)
    result.toArray
  }

  def buildCube(df: DataFrame, attributes: Array[String]): DataFrame = {
//    val cube = df.cube(attributes(0), StrUtils.withoutFirst(attributes))
    CubeAnalyzer.apply(df, attributes)
  }

  def countItems(df: DataFrame): DataFrame =
    df // .sqlContext.sql()

  def extractStrings(row: Row, attr: Array[Int]): Array[String] = {
    val result= new ArrayBuffer[String]
    for (col <- 0 to attr.length-1) {
      val s = row.getString(attr(col)) + " | " + col.toString;
      result.append(s)
    }
    result.toArray
  }

  def loadSignificantInfo(df: DataFrame, attr: Array[Int]): RDD[Array[String]] = {
    df.rdd.map(extractStrings(_,attr))
  }

  def FPGrowth(data: RDD[Array[String]]): Unit = {
    val rows = data.count.toDouble
    val weight = 100 / rows
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
    val model = fpg.run(data)
    val itemset = model.freqItemsets.collect()
//    println("============Itemsets====================")
//    itemset.foreach(println)
//    println("=========================================")
    itemset.foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + " " +
        (weight*itemset.freq).toInt + "%")
//      println("=========================================")
    }
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }

}

/*
 |-- MANDANT: integer (nullable = true)
 |-- HTZ: string (nullable = true)
 |-- BEN: string (nullable = true)
 |-- QUELLE_ORT: string (nullable = true)
 |-- ZIEL_ORT: string (nullable = true)
 |-- ORG: string (nullable = true)
 |-- KST: string (nullable = true)
 |-- LKS: integer (nullable = true)
 |-- TAG: integer (nullable = true)
 |-- STK: integer (nullable = true)
 |-- ME_TXT: string (nullable = true)
 |-- B_DATUM: string (nullable = true)
 MANDANT
 HTZ
 BEN
 QUELLE_ORT
 ZIEL_ORT
 ORG
 KST
 LKS
 TAG
 STK
 ME_TXT
 B_DATUM


*/
/*
    val olapCube = buildCube(df, attributes)
    val rdd = olapCube.rdd.filter(_.getLong(attributes.length) > minItems);
    CubeAnalyzer.showFilteredResults(rdd)
    olapCube.printSchema()
    val cols = Array(1, 2, 3);
    val data = loadSignificantInfo(df, cols)
    FPGrowth(data)
*/
