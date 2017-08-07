package works.gaina.bda.clustering

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaina on 1/28/16.
  *
  * Class describes changeable membership of points from dataset to different clusters during clustering.
  *
  * This is auxiliary class - no links to algorithms, measures of proximity and so on and should be used by clustering algorithms.
  *
  * Point in terms of clustering means a observation (case) in terms of stats or row of dataset in terms of database.
  * PointID is point identifier, number of row or generated key for row
  * ClusterId is cluster identifier
  *
  * @constructor create a new partition for a dataset to be clustered
  * @param dsPoints - number of points in clustered dataset
  */
class Membership (val dsPoints: Int) extends Serializable {
  val tagUnclustered = 0
  val firstClusterID = 1
  /** Array, each element is number of class for respective point
    * A(n) = k means point "n"  belongs to cluster "k" *
    * if k is equal to 0, point is unclustered yet */
  val body = Array.fill[Int](dsPoints)(tagUnclustered)
  /** Map, with key number of cluster and values - number of points in cluster  */
  val clustersCardinality = scala.collection.mutable.Map[Int, Int]()

  /**   */
  private def inClosedOpenRange(i:Int,low:Int,upp:Int) : Boolean = {
  (i >= low) && (i < upp)
}

  /**   */
  private def validPointID(pointID: Int): Boolean =
  inClosedOpenRange(pointID, 0 , dsPoints)

  private def validPointsID(pointsID: Array[Int]): Boolean = {
  var result = true
  var i = -1
  do {
  i += 1
  result = result && validPointID(pointsID(i))
} while (result && (i < pointsID.length - 1))
  result
}

  /** Check if point is valid and have non-null membership
    *
    * @param pointID mean ID of point of clustered dataset
    */
  def isPointClustered(pointID: Int): Boolean =
  validPointID(pointID) && (body(pointID) != tagUnclustered)

  def isPointUnclustered(pointID: Int): Boolean =
  !isPointClustered(pointID)

  /** Check if two points are valid and belong to the same cluster
    *
    * @param pointID1 mean ID of 1st point of clustered dataset
    * @param pointID2 mean ID of 2nd point of clustered dataset
    */
  def belongToSameCluster(pointID1: Int, pointID2: Int) =
  validPointID(pointID1) && validPointID(pointID2) &&
  (body(pointID1) == body(pointID2))

  def unclusteredPointsExists: Boolean =
  body.indexOf(tagUnclustered) != -1

  /** ID of clustered point after point with ID from  */
  def nextClusteredPoint(from: Int): Int = {
  var i: Int = from
  while((body(i) == tagUnclustered) && (i < body.size - 1))
  i += 1
  if (body(i) == tagUnclustered)
  i = -1
  i
}

  /** Check if cluster is valid   */
  def validClusterID(clusterID: Int): Boolean =
  clustersCardinality.get(clusterID) != None

  /** Returns the number of cluster, own point  */
  def belongsTo(pointID: Int): Int = {
  var result = tagUnclustered
  if (validPointID(pointID))
  result = body(pointID)
  result
}

  /** Returns number of clusters at the moment */
  def getClustersCount: Int =
  clustersCardinality.size

  /** Returns lowest non-used cluster number  */
  def getFreeClusterID: Int = {
  var freeClusterID: Int = firstClusterID
  if(getClustersCount > 0)
  while(validClusterID(freeClusterID)) {
  freeClusterID += 1
}
  freeClusterID
}

  /** Returns the number of points in cluster  */
  def clusterSize(clusterID: Int): Int = {
  clustersCardinality.get(clusterID) match {
  case Some(value) => value
  case None => 0
}
}

  /**   */
  private def incPointsInCluster(clusterID: Int, addedPoints: Int): Int = {
  val points = clusterSize(clusterID) + addedPoints
  clustersCardinality.put(clusterID, points)
  points
}

  /**   */
  private def incPointsInCluster(clusterID: Int): Int =
  incPointsInCluster(clusterID, 1)

  /**   */
  private def decPointsInCluster(clusterID: Int, excludedPoints: Int): Int = {
  val points = clusterSize(clusterID) - excludedPoints
  clustersCardinality.put(clusterID, points)
  points
}

  /**   */
  private def decPointsInCluster(clusterID: Int): Int =
  decPointsInCluster(clusterID, 1)

  /** Create a new empty cluster  */
  def openCluster: Int = {
  val newClusterID = getFreeClusterID
  clustersCardinality.put(newClusterID, 0)
  newClusterID
}

  /** Creates a new empty cluster and places point in this cluster  */
  def createCluster(pointID: Int): (Boolean, Int) = {
  val result = validPointID(pointID)
  var newClusterID: Int = tagUnclustered
  if(result) {
  newClusterID = openCluster
  placeIn(newClusterID, pointID)
}
  (result, newClusterID)
}

  def createCluster(pointsID: Array[Int]): (Boolean, Int) = {
  val result  = validPointsID(pointsID)
  var newClusterID: Int = tagUnclustered
  if(result) {
  newClusterID = openCluster
  placeIn(newClusterID, pointsID)
}
  (result, newClusterID)
}

  /** Remove any information about this cluster from partition   */
  def removeEmptyCluster(clusterID: Int): Boolean = {
  var result = true
  if(clusterSize(clusterID) == 0)
  clustersCardinality.remove(clusterID)
  else
  result = false
  result
}

  /** Places point in cluster and modify cardinality of this cluster */
  def placeIn(clusterID: Int, pointID: Int): Boolean = {
  val result = (validClusterID(clusterID) && validPointID(pointID))
  if(result) {
  body(pointID) = clusterID
  incPointsInCluster(clusterID)
}
  result
}

  /** Places several points in cluster and modify cardinality of this cluster  */
  def placeIn(clusterID: Int, pointsID: Array[Int]): Boolean = {
  var result = (validClusterID(clusterID))
  for (id <- pointsID)
  result = result && validPointID(id)
  if(result) {
  pointsID foreach {body(_) = clusterID}
  incPointsInCluster(clusterID, pointsID.size)
}
  result
}

  /** Remove point from  cluster  and modify cardinality of this cluster */
  def excludeFrom(pointID: Int, clusterID: Int): Boolean = {
  val result = (validPointID(pointID) && validClusterID(clusterID))
  if(result) {
  body(pointID) = tagUnclustered
  decPointsInCluster(clusterID)
}
  result
}

  /** Move point from one cluster to another and modify cardinality of both clusters  */
  def moveTo(pointID: Int, newClusterID: Int): Boolean = {
  val result = (validPointID(pointID) && validClusterID(newClusterID))
  if(result) {
  val oldClusterID = body(pointID)
  decPointsInCluster(oldClusterID)
  body(pointID) = newClusterID
  incPointsInCluster(newClusterID)
}
  result

}

  /** Places in the cluster with lowest ID the points from another clusters  */
  def mergeClusters(clustersID: Array[Int]): Unit = {
  if (clustersID.size > 1) {
  val targetCluster = clustersID.min
  val clustersToMergeID = clustersID.filter(_ != targetCluster)
  body.zipWithIndex.foreach {
  case (clusterID: Int, ind: Int) => {
  if (clustersToMergeID.contains(clustersID))
  moveTo(ind, targetCluster)
}
}
}
}

  /** Places the points of cluster with greater ID to another  */
  def mergeTwoClusters(clusterID1: Int, clusterID2: Int): Boolean = {
  val result: Boolean = validClusterID(clusterID1) && validClusterID(clusterID2)
  if (result) {
  val clustersID = new Array[Int](2)
  clustersID(0) = clusterID1
  clustersID(1) = clusterID2
  mergeClusters(clustersID)
}
  result
}

  /** Returns an array with ID ob observations from given cluster  */
  def getClusterPoints(clusterID: Int): Array[Int] = {
  val result = new ArrayBuffer[Int]
  var from: Int = 0
  var indOfPoint: Int = -1
  do {
  indOfPoint = body.indexOf(clusterID, from)
  if (indOfPoint > -1) {
  result.append(indOfPoint)
  from = indOfPoint + 1
}
} while ((indOfPoint != -1) && (from < body.size))
  result.toArray
}

  /** Returns the ID of clusters like a ascending sorted array */
  def getCliustersID: Array[Int] = {
  clustersCardinality.keySet.toArray.sorted
}

  /** Returns a string with all points ID of cluster, delimited with comma  */
  def getClusterPointsAsText(clusterID: Int): String = {
  "Cluster " + clusterID.toString + ": " + getClusterPoints(clusterID).mkString("[", "," , "]")
}

  /** Shows all clusters as a text. Each row is text representation of a cluster */
  def showClustersAsText:Unit = {
  val clustersID = getCliustersID
  for (id <- clustersID)
  println(getClusterPointsAsText(id))
}


}
