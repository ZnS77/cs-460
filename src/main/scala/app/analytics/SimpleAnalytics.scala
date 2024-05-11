package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    ratingsPartitioner = new HashPartitioner(ratings.partitions.length)
    moviesPartitioner = new HashPartitioner(movie.partitions.length)

    // movie: (movieId, title, keywords)
    titlesGroupedById = movie.groupBy(_._1).partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
    // ratings: (userId, movieId, prevRating, rating, timestamp)
    val ratingsGroupedByYear = ratings.groupBy{
      case (_, _, _, _, timestamp) =>
        val year = new DateTime(timestamp * 1000L).getYear
        year
    }.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)
    ratingsGroupedByYearByTitle = ratingsGroupedByYear.mapValues{x =>
      x.groupBy(_._2)
      }.persist(MEMORY_AND_DISK)
    }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    ratingsGroupedByYearByTitle.mapValues(_.size)
  }

  def helper_getMostRatedMovieIdEachYear: RDD[(Int, Int)] = {
    //return as movieId, year
    val most_rated = ratingsGroupedByYearByTitle.mapValues{ x =>
      // x: movieId, List[(userId, movieId, prevRating, rating, timestamp)]
      val maxCount = x.map(_._2.size).max
      val mostRatedCandidates = x.filter(_._2.size == maxCount)
      mostRatedCandidates.maxBy(_._1)._1
    }
    most_rated.map{case(a,b) => (b,a)}
  }
  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val yearMoviePair= helper_getMostRatedMovieIdEachYear
    val joined = titlesGroupedById.join(yearMoviePair)
    joined.map{case(id, (movieData, year)) => (year, movieData.head._2)}
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val yearMoviePair = helper_getMostRatedMovieIdEachYear
    val joined = titlesGroupedById.join(yearMoviePair)
    joined.map{case(id, (movieData, year)) => (year, movieData.head._3)}
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getLeastAndMostRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val genreCount = getMostRatedGenreEachYear.flatMap(_._2).map(x => (x, 1)).reduceByKey(_ + _)
    val genreCountSorted = genreCount.collect().sortBy(x => (x._2, x._1))
    val leastRatedGenre = genreCountSorted.head
    // genreCountSorted is sorted in ascending order, so the last element is lexicographically the largest
    val mostRatedGenre = genreCountSorted.filter(_._2 == genreCountSorted.last._2).head
    (leastRatedGenre, mostRatedGenre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val requiredGenresSet = requiredGenres.collect().toSet
    movies.filter{ case (_, _, genres) =>
      genres.exists(requiredGenresSet.contains)
    }.map(_._2)
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val broadcastGenres = broadcastCallback(requiredGenres)
    movies.filter{ case (_, _, genres) =>
      genres.exists(broadcastGenres.value.contains)
    }.map(_._2)
  }

}

