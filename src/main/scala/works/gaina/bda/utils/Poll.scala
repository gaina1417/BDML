package works.gaina.bda.utils

import scala.collection.mutable.ArrayBuffer

class Poll[T]() extends Serializable {

  val state = scala.collection.mutable.Map[T, Int]()

  def getVotes(candidate: T): Int = state(candidate)

  def vote(candidate: T): Unit = {
    state.update(candidate, state.getOrElse(candidate, 0) + 1)
  }

  def groupVoting(candidates: Array[T]): Unit = {
    candidates.foreach(vote)
  }

  def getVotingWinner: T = state.maxBy(_._2)._1
  def getWinnerVotes: Int = state.maxBy(_._2)._2
  def getVotingLoser: T = state.minBy(_._2)._1
  def getLoserVotes: Int = state.minBy(_._2)._2

  def getVotes: Array[Int] = {
    val result = new ArrayBuffer[Int]
    state.keys.foreach(result += getVotes(_))
    result.toArray
  }

  def getVotesAscending:Array[Int] = getVotes.sorted
  def getVotesDescending:Array[Int] = getVotes.sorted.reverse

}
