package app.recommender

import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import app.recommender.LSH.{LSHIndex, NNLookup}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  private def helper_getNNMoviesId(genre: List[String]): List[Int] = {
    val genreRDD = sc.parallelize(List(genre))
    val moviesRelated = nn_lookup.lookup(genreRDD).flatMap(x => x._2).map(x => x._1).collect().toList
    moviesRelated
  }

  private def filterWatched(userId: Int, movieIds: List[Int]): List[Int] = {
    val watched = baselinePredictor.movieWatched(userId)
    movieIds.filterNot(watched.contains(_))
  }
  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] ={
    val movieIdsToRate = helper_getNNMoviesId(genre)
    val candidates = filterWatched(userId, movieIdsToRate)
    val ratings = candidates.map(movieId => (movieId, baselinePredictor.predict(userId, movieId)))
    ratings.sortBy(-_._2).take(K)
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val movieIdsToRate = helper_getNNMoviesId(genre)
    val candidates = filterWatched(userId, movieIdsToRate)
    val ratings = candidates.map(movieId => (movieId, collaborativePredictor.predict(userId, movieId)))
    ratings.sortBy(-_._2).take(K)
  }
}
