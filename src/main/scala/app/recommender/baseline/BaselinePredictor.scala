package app.recommender.baseline

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK


class BaselinePredictor() extends Serializable {
  private var state = null
  private var userDeviated: RDD[(Int, (Int, Double))] = null // userId, ratingCounts, rMean
  private var ratingsDeviated: RDD[(Int, (Int, Double, Double))] = null // userId, movieId, rOriginal, rDeviated
  private var movieDeviated: RDD[(Int, Double)] = null // movieId, rDeviated
  private def helper_scale(rNow: Double, rMean:Double):Double ={
    if (rNow == rMean)
    return 1.0
    else if (rNow > rMean)
      return 5 - rMean
    else
      return rMean -1
  }
  def getDeviatedRating(rOriginal: Double, rMean: Double): Double = {
    return (rOriginal - rMean)/ helper_scale(rOriginal, rMean)
  }

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val latestRatings =  ratingsRDD.groupBy{
      case (userId, movieId, _, _, ts) => (userId, movieId)
    }.mapValues(x => x.toList.maxBy(_._5))
      .map { x => (x._1._1, (x._2._2, x._2._3, x._2._4, x._2._5)) }
    // (userId, (movieId, prevRating, rating, timestamp))
    userDeviated = latestRatings.mapValues(x=>x._3).groupBy(_._1).mapValues{ x =>
      val ratingCounts = x.size
      val rMean = x.map(_._2).sum / ratingCounts
      (ratingCounts, rMean)
    }.persist(MEMORY_AND_DISK)
    // (userId, (ratingCounts, rMean))
    ratingsDeviated = latestRatings.join(userDeviated).map{
      case (userId, ((movieId, _, rOriginal, _), (_, rMean))) =>
        (userId, (movieId, rOriginal, getDeviatedRating(rOriginal, rMean)))
    }.persist(MEMORY_AND_DISK)
    movieDeviated = ratingsDeviated.map{
      case (_, (movieId, _, rDeviated)) => (movieId, rDeviated)
    }.groupBy(_._1).mapValues(x => x.map(_._2).sum / x.size)
      .persist(MEMORY_AND_DISK)

  }

  def predict(userId: Int, movieId: Int): Double = {
    val ubias = userDeviated.lookup(userId).headOption.getOrElse((0, 0.0))._2
    val mbias = movieDeviated.lookup(movieId).headOption.getOrElse(0.0)
    ubias + mbias * helper_scale((ubias + mbias), ubias)
  }

  def movieWatched(userId: Int): List[Int]= {
    val movies = ratingsDeviated.lookup(userId)
    if movies.isEmpty then
      List()
    else {
      movies.map(_._1).toList
    }
  }


}
