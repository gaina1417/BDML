package works.gaina.bda.app

import java.util

import scala.collection.mutable
import works.gaina.bda.task.InfoPanel._
import java.util.BitSet

/**
  * Created by gaina on 6/1/17.
  */
object Test  extends App {

  val i1 = solution(1041)
  println(i1)

  def testBitset1: Unit = {
    showInfo("test1 - Scala BitSet")
    val src1 = Array[Long](17)
    val src2 = Array[Long](1)
    val bm1 = new mutable.BitSet(src1)
    val bm2 = new mutable.BitSet(src2)
    val bm = bm1 & bm2
    println(bm1.toString)
    println(bm2.toString)
    println(bm.toString)
  }

  def testBitset2: Unit = {
    showInfo("test2 - Java BitSet")
    val bm1 = new util.BitSet(16)
    bm1.set(0)
    bm1.set(4)
    val bm2 = new util.BitSet(16)
    bm2.set(4)
    val bm = bm1 and(bm2)
    println(bm1.toString)
    println(bm2.toString)
    println(bm.toString)
  }

  def test3: Unit = {
    val size = 10
    val b = new Array[Long](size)
  }
//  BinaryGap

  def toBits(n: Int): BitSet = {
    val bs = new BitSet()
    var b = 1
    for (i <- 0 to 31) {
      if ((n & b) == b)
        bs.set(i)
      b = b << 1
    }
    bs
  }

  def solution(n: Int): Int = {
    var maxGap = 0
    val bs = toBits(n)
    var pos1 = bs.nextSetBit(0)
    if (pos1 != -1)
      do {
        val pos2 = bs.nextSetBit(pos1 + 1)
        if (pos1 != -1)
          if (maxGap < (pos2 - pos1 - 1))
            maxGap = pos2 - pos1 - 1
        pos1 = pos2
      } while((pos1 > -1) && (pos1 < 31))
    maxGap
  }


}
