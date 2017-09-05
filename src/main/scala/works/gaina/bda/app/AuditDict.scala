package works.gaina.bda.app

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.types._

object AuditDict extends Serializable {
  val localApp: Boolean = true
  val word = "loan"
  val words = Array("loan", "audit", "credit")
  var maxSynonyms = 100
  var spark: SparkSession = null
  case class AuditDictItem(term: String, abbreviation: String, explanation: String)
  val workDir = "/home/gaina/Data/KPMG/"
  val fName = "AuditDict.txt"
  val vectorsfName = "numWords.csv"
  val path = workDir + fName
  val outPath = workDir + vectorsfName
  val outTableName = "auditdict"
  val hostIP = "192.168.8.100"
  val master = "local[*]"
  val valdPrefix = "<P>"
  val valdSuffix = "</P>"
  def abbrLeftTag = '('
  def abbrRightTag = ')'
  def referenceTag = '*'
  def horizTab = "\u0009"

  def main(args: Array[String]): Unit = {
    spark = createSparkSession
    val items = loadData(path)
//    items.foreach(println)
//    val lines = items.count
//    println(s"Dictionary contains $lines audit terms")
//    items.foreach(tokenizeTerm(_))
    val model = quantifyText(items)
//    exportNumbersWord2Vec(model, vectorsfName)
//    saveModel(spark, model, workDir)
    writeToOutTable(args, model,outTableName)
  }

  def createSparkSession: SparkSession =
    if (localApp)
      SparkSession.builder()
        .master(master)
        .appName("AuditDict")
        .config("spark.driver.host", hostIP)
        .getOrCreate()
    else
      SparkSession.builder()
        .appName("AuditDict")
        .getOrCreate()

  def stringIsValid(s: String): Boolean =
    (s.length > 10) && s.startsWith(valdPrefix) && s.endsWith(valdSuffix)

  def removePrefixSuffix(s: String): String = {
    val s1 = s.substring(valdPrefix.length)
    s1.substring(0,s1.length-valdSuffix.length-1)
  }

  def removeSeparators(s: String): String =
    s.replace(",","").replace(".","").replace("*","")

  def removeUselessChars(s: String): String =
    removeSeparators(removePrefixSuffix(s))

  def loadData(fullPath: String): RDD[String] = {
    val ds = spark.read.textFile(fullPath)
    ds.filter(stringIsValid(_)).rdd.map(removeUselessChars(_))
  }

  def getAbbreviaton(s: String): String = {
    var abbr = ""
    if (s.indexOf(referenceTag) == -1) {
      val pos1 = s.indexOf(abbrLeftTag)
      val pos2 = s.indexOf(abbrRightTag)
      if ((pos1 != -1) && (pos2 != -1) && (pos1+1 < pos2-1))
        abbr = s.substring(pos1, pos2 + 1)
    }
    abbr
  }

  def wordInSmallLetters(s: String): Boolean =
    s.toLowerCase == s

  def wordStartsWithCapitalLetter(s: String): Boolean = {
    val ch = s.charAt(0)
    (ch >= 'A') && (ch <= 'Z')
  }

  def removeExtremesChars(s: String): String =
    s.substring(1, s.length - 1)

  def tokenizeTerm(sentence: String):AuditDictItem = {
    println(s"$sentence")
    var term = ""
    var abbreviation = ""
    var explanation = ""
    var result = -1
    val words = sentence.split(" ").filter(_ != "")
    var multiExplanation: Boolean = false

    var l = -1
    do {
      l += 1
      if (wordStartsWithCapitalLetter(words(l)))
        result = l
      if (words(l).charAt(0) == '1') {
        result = l - 2
        multiExplanation = true
      }
    } while ((result == -1) && (l < words.length -1))
    if ((result != -1) && (!multiExplanation))
    do {
      l += 1
      if (wordInSmallLetters(words(l)))
        result = l
    } while ((result == -1) && (l < words.length -1))
    if (result != -1) {
      val extterm = words.slice(0, l-1).mkString(" ")
      abbreviation = getAbbreviaton(extterm)
      if (abbreviation != "") {
        val t = extterm.replaceAll(abbreviation, "")
        term = t.substring(0, t.length - 3)
        abbreviation = removeExtremesChars(abbreviation)
      }
      else
        term = extterm
      explanation = words.slice(l-1, words.length).mkString(" ")

    }
    println(s"Term = $term")
    println(s"abbreviation = $abbreviation")
    println(s"explanation = $explanation")
    println("==============================================")
    println
    AuditDictItem(term, abbreviation, explanation)
  }

  def quantifyText(rdd: RDD[String]): Word2VecModel = {
    val input = rdd.map(_.split(" ").toSeq)
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)
//    val synonyms = model.findSynonyms("audit", 17)
    val synonyms = model.findSynonyms(word, maxSynonyms)
    val v = model.transform(word)
    val size = v.size
/*
    println("==================== INPUT =====================")
    println(s"Word #$word# represented by vector of size $size")
    println(v)
    println("======== SYNONYMS AND COUSINE DISTANCES ========")
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
    println("================================================")
*/
    model
  }

  def word2VecModelToDataFrame(model: Word2VecModel,
                               includeWords: Boolean = true): Unit = {
    val fieldsDescriptor = "STR=>FLT[]"
//    val colNames =
//    val schema = SchemaBuilder.createSchema()
  }

  def saveModel(model: Word2VecModel, path: String): Unit = {
    model.save(spark.sparkContext, path)
  }

  def exportNumbersWord2Vec(model: Word2VecModel, fName: String) = {
    val vectors = (model.getVectors.map(_._2)).map(_.mkString(",")).toArray
    spark.sparkContext.parallelize(vectors).saveAsTextFile(fName)
    for (s <- vectors)
      println(s)
  }

  def validArgument(arg: String,  model: Word2VecModel): Boolean = {
    val opt = model.getVectors.get(arg)
    opt != None
  }

  def validateArgs(args: Array[String], model: Word2VecModel): Array[String] =
    args.filter(validArgument(_, model))

  def rddFromSynonym(arg: String, model: Word2VecModel): RDD[Row] = {
    val synonims = model.findSynonyms(arg, maxSynonyms)
    val rows = synonims.map(x => Row(arg, x._1, x._2))
    spark.sparkContext.parallelize(rows)
  }

  def writeToOutTable(args: Array[String], model: Word2VecModel, tblName: String): Unit = {
    val validArgs = validateArgs(args, model)
    if (validArgs.length > 0) {
      var rdd = rddFromSynonym(validArgs(0), model)
      for (i <- 1 to validArgs.length - 1)
        rdd = rdd.++(rddFromSynonym(args(i), model))
      val schema = createDFOutSchema
      val df = spark.createDataFrame(rdd, schema)
      df.show(df.rdd.count.toInt)
      createHiveOutTable(tblName)
      df.write.mode("append").saveAsTable(tblName)
    }
  }

  def createDFOutSchema: StructType = {
    StructType(
      StructField("Word", StringType, false) ::
      StructField("Synonym", StringType, false) ::
      StructField("CousineDistance", DoubleType, false) :: Nil)
  }

  def createHiveOutTable(tblName: String): Unit = {
    val query =
      """CREATE TABLE IF NOT EXISTS mydb.employees (
        |word STRING comment 'Source word',
        |synonym STRING comment 'It synonym',
        |cousinesistance FLOAT comment 'Measure of proximity');
      """.stripMargin
  }

}
