package works.gaina.bda.clustering

/**
  * Created by gaina on 5/30/17.
  */
class HeadTailBreaks[Double](data: Array[Double]) {

//   val sample = data.sorted

  def allValuesAreEqual(sortedSample: Array[Double]): Boolean = {
    val len = sortedSample.length
    if ((len > 1) && (sortedSample(0) != sortedSample(len-1)))
      false
    else
      true
  }

/*
  def nextLayer(y: Array[T], currThreshold: T): Array[T] =
    y.filter(_ > currThreshold)

def get: Array[Double] = {
    val result = new ArrayBuffer[Double]
    var sample = data.clone()
    do {
      val layer = nextLayer(sample)
      currThreshold = layer.sum / layer.length
      result.append(currThreshold)
      sample = layer
    } while(!stoppingRule(sample))
    result.toArray
  }


  def exceedsThreshold(x: Double) : Boolean = {
    if (x > currThreshold)
      true
    else
      false
  }

}

 */

}
