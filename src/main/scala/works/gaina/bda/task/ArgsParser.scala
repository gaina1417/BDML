package works.gaina.bda.task

/**
  * Created by gaina on 12/16/16.
  */
class ArgsParser(args: Array[String], keywordHahTag: String = "--") {

  val keywords = indexesOfKeywords.map(args(_))
  val keywordsIndexes = indexesOfKeywords :+ args.length

  def indexesOfKeywords: Array[Int] =
    args.zipWithIndex.filter(_._1.trim().startsWith(keywordHahTag)).map(_._2)

  def isValidKey(keyword: String): Boolean =
    (keywords.indexOf(keywordHahTag + keyword)) != -1

  def getValues(keyword: String): Array[String] = {
    val l = keywords.indexOf(keywordHahTag + keyword)
    if ((l != -1) && (keywordsIndexes(l) + 1 <= keywordsIndexes(l + 1)))
      args.slice(keywordsIndexes(l) + 1, keywordsIndexes(l + 1))
    else
      new Array[String](0)
  }

}
