package works.gaina.bda.utils

/**
  * Created by gaina on 12/13/16.
  */
object NumUtils {

  def inOpenRange(i:Int,low:Int,upp:Int) : Boolean = {
    (i > low) && (i < upp)
  }

  def inClosedRange(i:Double,low:Double,upp:Double) : Boolean = {
    (i >= low) && (i <= upp)
  }

  def inOpenClosedRange(i:Int,low:Int,upp:Int) : Boolean = {
    (i > low) && (i <= upp)
  }

  def inClosedOpenRange(i:Int,low:Int,upp:Int) : Boolean = {
    (i >= low) && (i < upp)
  }

  def medianIndex(sampleSize: Int): (Int, Int) = {
    var i1: Int = 0
    var i2: Int = 0
    if ((sampleSize % 2) == 0) {
      i1 = sampleSize / 2 - 1
      i2 = i1 + 1
    }
    else {
      i1 = (sampleSize - 1) / 2
      i2 = 0
    }
    (i1,i2)
  }

  def indexingArray(data: Array[Double]): Array[Int] = {
    val mappedData = data.zipWithIndex
    mappedData.sorted.map(x => x._2)
  }

  def indexingArray(data: Array[Int]): Array[Int] = {
    val mappedData = data.zipWithIndex
    mappedData.sorted.map(x => x._2)
  }

  def logarithm(base: Double, x: Double): Double =
    math.log(x) / math.log(base)

  def roundUp(d: Double) = math.ceil(d).toInt

}
