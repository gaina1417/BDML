package works.gaina.bda.app
import breeze.linalg._, eigSym.EigSym

/**
  * Created by gaina on 2/27/17.
  */
object PCAtest {
  def main(args: Array[String]): Unit = {
    val A = createTest
    val es = eigSym(A)
    val lambda = es.eigenvalues
    val evs = es.eigenvectors
    println(s"Eigenvalues $lambda")
    println("=========== Eigenvectors ============")
    for (i <- 0 to evs.rows - 1) {
      val v = evs(::, i)
      println(s"Eigenvector $i => $v")
    }
    println("=========== Eigenvectors ============")
  }

  def createTest: DenseMatrix[Double] = {
    DenseMatrix((9.0,0.0,0.0),(0.0,82.0,0.0),(0.0,0.0,25.0))
  }
}
