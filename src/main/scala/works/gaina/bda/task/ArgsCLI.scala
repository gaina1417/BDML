package works.gaina.bda.task

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import works.gaina.bda.spark.SchemaBuilder

import scala.collection.mutable.ArrayBuffer

/**
  * Command Line Interpreter for Scala application arguments in SparkSQL Row mode.
  * Class parse arguments of application, detect keywords and their values, create the schema for keywords data types,
  * fill an Array of values and create a SparkSQL Row with parameters.
  *
  * @constructor create a SparkSQL Row, wich provide the values for parameters like params.get(i)
  * @param typesDescr - string describes the type of arguments in given order
  * @param keywordsDescr - string describes the keywords in given order
  * @param requiredDescr - string describes the mandatory and parameters, having default value
  * @param args - arguments of application provided by Scala
  * @param keywordHahTag -
  */
class ArgsCLI(typesDescr: String, keywordsDescr: String, requiredDescr: String, args: Array[String],
              keywordHahTag: String = "--") {

  val schema = SchemaBuilder.createSchema(typesDescr, keywordsDescr, requiredDescr)
  lazy val keywordsWithLimits = getKeywords
  lazy val keywords = keywordsWithLimits._1
  lazy val limits = keywordsWithLimits._2
  val params = getParams
  var status: Int = 0

  /**
    *
    * @return keywords, values and nullability as a SparkSQL row
    */
  def getParams: Row = {
    val values = parse
    if ((schema != null) && (values.length == schema.fields.length))
      new GenericRowWithSchema(values, schema)
    else
    {
      status = -1
      null
    }
  }

  /**
    * Check if argument is predefined keyword
    * @param arg - a keyword in right case
    * @return true if argument is keyword, false otherwise
    */
  def isKeyword(arg: String): Boolean =
    arg.indexOf(keywordHahTag) == 0

  /**
    * Returns next keyword in argument list
    * @param from - starting position for search
    * @return keyword ant it index in arguments array
    */
  def nextKeyword(from: Int): (String, Int) = {
    var keyword: String = ""
    var i: Int = from
    do {
      if (isKeyword(args(i)))
        keyword = args(i)
      else
        i += 1
    } while((keyword == "") && (i < args.length - 1))
    if (keyword == "")
      i = -1
    else
      keyword = keyword.replaceFirst(keywordHahTag, "").replaceAll(" ", "").toUpperCase
    (keyword, i)
  }

  /**
    * @return all keywords as values and indexes in arguments array
    */
  def getKeywords: (Array[String], Array[Int]) = {
    var next: (String, Int) = ("", -1)
    val keys = new ArrayBuffer[String]
    val inds = new ArrayBuffer[Int]
    var from = 0
    do {
      next = nextKeyword(from)
      if (next._2 != -1) {
        keys.append(next._1)
        inds.append(next._2)
        from = next._2 + 1
      }
    } while ((next._2 != -1) && (from < args.length - 1))
    inds.append(args.length)
    (keys.toArray, inds.toArray)
  }

  /**
    *
    * @param keywordID - keyword ID
    * @return argument values by keyword identifier
    */
  def getArgValues(keywordID: Int): Array[String] = {
    val l1 = limits(keywordID) + 1
    val l2 = limits(keywordID + 1)
    args.slice(l1, l2)
    //    args.slice(keywords._2(keywordID) + 1, keywords._2(keywordID + 1) - 1)
  }

  def getArgValues(keyword: String): Array[String] = {
    val id = keywords.indexOf(keyword)
    if (id != -1)
      getArgValues(id)
    else
      null
  }

  /**
    *
    * @return values of arguments as a array
    */
  def parse: Array[Any] = {
    val result = new ArrayBuffer[Any]
    for (field <- schema.fields) {
      val name = field.name.replaceAll(" ", "").toUpperCase
      val dtype = field.dataType
      val id = keywords.indexOf(name)
      if (id != -1) {
        val values = getArgValues(id)//.map(_.asInstanceOf[dType])
        if (values.length == 1)
          result.append(values(0))
        else
          result.append(values)
      }
    }
    result.toArray
  }

}
