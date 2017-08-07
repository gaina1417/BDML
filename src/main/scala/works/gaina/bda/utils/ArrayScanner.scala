package works.gaina.bda.utils

/**
  * Created by gaina on 12/20/16.
  */
class ArrayScanner[T] {

  def indexesWhere(data: Array[T], p: (T) ⇒ Boolean, from: Int = 0): Array[Int] =
    data.slice(from, data.length).zipWithIndex.filter(t => (p(t._1) == true)).map(_._2 + from)

/*

  def indexOfMin(data: Array[T]): Int =
    (data.zipWithIndex).minBy(_._1)._2


  def indexOfMax(data: Array[T]): Int =
    (data.zipWithIndex).maxBy(_._1)._2
*/

}
