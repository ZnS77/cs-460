package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner = null
  private var aggregated: RDD[(Int, (String, Double, Int, List[String],
    List[(Int, Option[Double], Double, Int)]))] = null
  // (movieId, (title, average_rating, ratingCounts, keywords,
  //       List[(userId, prevRating, rating, timestamp)])

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val latestRatingPerUserTitle = helper_getLatestRatingPerUserTitle(ratings).groupByKey()
    // (movieId, List[(userId, prevRating, rating, timestamp)])

    aggregated = title.map {
      case (movieId, title, keywords) => (movieId, (title, keywords))
    }.leftOuterJoin(latestRatingPerUserTitle).map {
      case (movieId, ((title, keywords), ratingsOption)) =>
        val ratings = ratingsOption.getOrElse(List()).toList
        val ratingCounts = ratings.size
        val avgRating = if (ratingCounts == 0) 0.0
            else ratings.map(_._3).sum / ratingCounts
        (movieId, (title, avgRating, ratingCounts, keywords, ratings))
    }.persist(MEMORY_AND_DISK)
    // for movies without rating data,
    // (movieId, (title, 0.0, 0, keywords, List()))

  }

  private def helper_getLatestRatingPerUserTitle(ratings: RDD[(Int, Int, Option[Double], Double, Int)]):
  RDD[(Int, (Int, Option[Double], Double, Int))] =
    ratings.groupBy {
      case (userId, movieId, _, _, ts) => (userId, movieId)
    }.mapValues(x => x.toList.maxBy(_._5)) //ascending order of timestamp, get the latest one
      .map{x => (x._1._2, (x._2._1, x._2._3, x._2._4, x._2._5))}
  // (movieId, (userId, prevRating, rating, timestamp))

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    aggregated.map {
      case (_, (title, avgRating, _, _, _)) => (title, avgRating)
    }
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val moviesWithKeywords = aggregated.filter {
      case (_, (_, _, _, movieKeywords, _)) => keywords.forall(movieKeywords.contains(_))
    }.map {
      case (movieId, (_, avgRating, _, movieKeywords, _)) => (movieId, movieKeywords, avgRating)
    }
    val groupedKeywordMovies: RDD[(String, List[(Int, Double)])] = moviesWithKeywords.flatMap {
      case (movieId, movieKeywords, avgRating) => movieKeywords.map(keyword => (keyword, (movieId, avgRating)))
    }.groupByKey().mapValues(_.toList)
    val keywordRDD = sc.parallelize(keywords).map(x => (x, 0.0))
    val joined = keywordRDD.leftOuterJoin(groupedKeywordMovies)
    // (keyword, (0.0, Some(List[(movieId, avgRating)])))
    val avgRatings = joined.mapValues{
      case (0.0, None) => -1.0 //no title with the given keyword
      case (0.0, moviesRatings) => moviesRatings.get.map(_._2).sum / moviesRatings.get.size
    }
    //to debug
//    avgRatings.foreach(println)
    val avgRating = avgRatings.values.sum / keywords.size

    avgRating
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   *              format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val delta = sc.parallelize(delta_)
    val latestDeltaRatings = helper_getLatestRatingPerUserTitle(delta).groupByKey()
    // (movieId, List[(userId, prevRating, rating, timestamp)])

    aggregated = aggregated.leftOuterJoin(latestDeltaRatings).map {
      case (movieId, ((title, avgRating, ratingCounts, keywords, ratings), deltaRatingsOption)) =>
        val deltaRatings = deltaRatingsOption.getOrElse(List()).toList
        val newRatings = ratings ++ deltaRatings
        // List(userid, prevRating, rating, timestamp)
        // drop duplicate ratings
        val ratingsToKeep = newRatings.groupBy(_._1).view.mapValues(x=>x.maxBy(_._4)).values.toList
        val newRatingCounts = ratingsToKeep.size
        val newAvgRating = if (newRatingCounts == 0) 0.0
        else ratingsToKeep.map(_._3).sum / newRatingCounts
        (movieId, (title, newAvgRating, newRatingCounts, keywords, newRatings))
    }
    // to debug
    // aggregated.foreach(println)

  }



}

