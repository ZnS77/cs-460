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
    val latestRatingPerUserTitle = helper_getLatestRatingPerUserTitle(ratings)
    // ((userId, movieId), (userId, prevRating, rating, timestamp))
    val averageRatingPerTitle = latestRatingPerUserTitle.map {
      case ((userId, movieId), (_, _, _, rating, _)) => (movieId, rating)
    }.groupByKey().mapValues(x => (x.sum / x.size, x.size))
    // (movieId, (average_rating, ratingCounts))

    val aggregatedMovies = title.map {
      case (movieId, title, keywords) => (movieId, (title, keywords))
    }.leftOuterJoin(averageRatingPerTitle).map {
      case (movieId, ((title, keywords), rateAndCount)) =>
        (movieId, (title, rateAndCount.getOrElse((0.0, 0)), keywords))
    }.mapValues(x => (x._1, x._2._1, x._2._2, x._3))
    // (movieId, (title, average_rating, ratingCounts, keywords))
    val ratingsByMid = ratings.map {
      case (userId, movieId, prevRating, rating, ts) => (movieId, (userId, prevRating, rating, ts))
    }.groupByKey().mapValues(x => x.toList.sortBy(_._4))
    // (movieId, List[(userId, prevRating, rating, timestamp)]) sorted by timestamp
    aggregated = aggregatedMovies.leftOuterJoin(ratingsByMid).map {
      case (movieId, ((title, avgRating, ratingCounts, keywords), ratings)) =>
        (movieId, (title, avgRating, ratingCounts, keywords, ratings.getOrElse(List())))
    }

  }

  private def helper_getLatestRatingPerUserTitle(ratings: RDD[(Int, Int, Option[Double], Double, Int)]):
  RDD[((Int, Int), (Int, Int, Option[Double], Double, Int))] =
    ratings.groupBy {
      case (userId, movieId, _, _, ts) => (userId, movieId)
    }.mapValues(x => x.toList.maxBy(_._5)) //ascending order of timestamp, get the latest one

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
      case (_, (_, _, _, movieKeywords, _)) => movieKeywords.containsSlice(keywords)
    }.map {
      case (movieId, (_, avgRating, _, movieKeywords, _)) => (movieId, movieKeywords, avgRating)
    }
    val groupedKeywordMovies: RDD[(String, List[(Int, Double)])] = moviesWithKeywords.flatMap {
      case (movieId, movieKeywords, avgRating) => movieKeywords.map(keyword => (keyword, (movieId, avgRating)))
    }.groupByKey().mapValues(_.toList)
    val keywordRDD = sc.parallelize(keywords).map(x => (x, 0.0))
    val joined = keywordRDD.leftOuterJoin(groupedKeywordMovies)
    // (keyword, (0.0, Some(List[(movieId, avgRating)])))
    val avgRating = joined.map {
      case (_, (0.0, None)) => -1.0 //no title with the given keyword
      case (_, (0.0, moviesRatings)) => moviesRatings.get.map(_._2).sum / moviesRatings.get.size
    }.sum() / keywords.size
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
    val latestDeltaRatingPerUserTitle = helper_getLatestRatingPerUserTitle(delta)
      .map { x => (x._1._2, (x._2._1, x._2._3, x._2._4, x._2._5)) }

  val deltaGroupByMid = latestDeltaRatingPerUserTitle.groupByKey().mapValues(x => x.toList.sortBy(_._4))
  val updatedAggregated = aggregated.leftOuterJoin(deltaGroupByMid).map {
    case (movieId, ((title, avgRating, ratingCounts, keywords, ratings), deltaRatings)) =>
      val newRatings = deltaRatings.getOrElse(List())
      val updatedRatings = ratings ++ newRatings
      val updatedRatingCounts = ratingCounts + newRatings.size
      val updatedAvgRating = if (updatedRatingCounts == 0) 0.0
      else updatedRatings.map(_._3).sum / updatedRatingCounts
      (movieId, (title, updatedAvgRating, updatedRatingCounts, keywords, updatedRatings))
    }
  aggregated = updatedAggregated
  }



}

