package works.gaina.bda.common

/**
  * Created by gaina on 12/13/16.
  */
import org.apache.spark.sql.Row

object Descriptors extends Serializable {

  /** Describes types of attribute by distribution category  */
  object AttributeCategories extends Enumeration {
    type AttributeCategories = Value
    val EMPTY, DISCRETE, CONTINUOUS, MIXTURE, DISCONTINUOUS, COMPOSITE = Value
  }

  object MatrixTypes extends Enumeration {

    type MatrixTypes = Value

    val NORMAL, SQUARE, SYMMETRICWITHDIAGONAL, SYMMETRICWITHOUTDIAGONAL,
    LOWERTRIANGULAR, UPPERTRIANGULAR  = Value

  }

  object DistanceTypes extends Enumeration {

    type DistanceTypes = Value

    val EUCLIDEAN, HAMMING, MINKOWSKI, CHEBYSHEV, SORENSEN, GOWER, SOERGEL, KULCZYNSKI, CANBERRA, LORENTZIAN,
    INTERSECTION, WAVEHEDGES,CZEKANOWSKI = Value

  }

  /** Describes most popular types of distributions, both discrete and continuous */
  object DistributionsTypes extends Enumeration {
    type DistributionsTypes = Value
    val BETA, CAUCHY, CHISQUARED, EXPONENTIAL, FISHER, GAMMA, GUMBEL, LAPLACE, LEVY, LOGNORMAL,
    NAKAGAMI, NORMAL, PARETO, STUDENT, TRIANGULAR, UNIFORM, WEIBULL = Value

  }

  /** Describes names of most popular rule to choose number of bins in histogram by sample size */
  object HistogramBinsChoiceRules extends Enumeration {
    type HistogramBinsChoiceRules = Value
    val SQRT, STURGES, RICE, DOANE, SCOTT = Value
  }

  /** Describes categories of missing data in a sample */
  object MissingDataCategories extends Enumeration {
    type MissingDataCategories = Value
    val NOTFOUND, NEGLIGIBLEPART, STATSIGNIFICANTPART, SIGNIFICANTPART, MORETHANSIGNIFICANTPART, ALMOSTCOMPLETELY, COMPLETELY = Value
  }

  object SourceTypes extends Enumeration {
    type SourceTypes = Value
    val CSV, PARQUET, JSON, HIVE, JDBC = Value
  }

  object GridType extends Enumeration {
    type GridType = Value
    val UNSETTLED, UNIFORM, REGULAR, RECTANGULAR = Value
  }

  case class MatrixIndex(row: Int, col: Int)
  /** The threshold for the sample and the number of items does not exceed the  */
  case class LayerOfDensity(threshold: Double, size: Int)
  /** Descriptor of category of attribute and value of quantitative measure for this category */
  case class AttributeRank(typeOf: AttributeCategories.AttributeCategories, measure: Double)
  /** Common statisitics for dataset   */
  case class DataFrameStatistics(df_id: String, rows: Long, cols: Int, cells: Long, nulls: Long, nullsRatio: Double, voids: Long, voidsRatio: Double)
  /** Counts of voids values for given string colimns of a dataset   */
  case class DataFrameVoids(df_id: String, colsList: Array[Int], voids: Array[Long])
  /** Difference between 2 arrays with string descriptors  */
  case class ArraysDifference(areTheSame: Boolean, diff1st2nd: Array[String],  diff2nd1st: Array[String])
  case class ClusterLabeledPoint(clusterID: Int, features: Row)

}
