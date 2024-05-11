package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] = {
    val distFile = sc.textFile("./src/main/resources/" + path)
    val ratings = distFile.map(line =>
      val fields = line.split("\\|")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toInt)
    )
    val groupedRatings = ratings.groupBy {
      case (userId, movieId, _, _) => (userId, movieId)
    }
    val sortedRatings = groupedRatings.mapValues(x => x.toList.sortBy(_._4))
    //(user_id, movie_id), List[(user_id, movie_id, rating, timestamp)]
    val ratingsWithPre = sortedRatings.mapValues(x => {
      //x: List[(user_id, movie_id, rating, timestamp)]
      x.zipWithIndex.map {
        case ((userId, movieId, rating, timestamp), index) =>
          val prevRating = if (index == 0) None else Some(x(index - 1)._3)
          (userId, movieId, prevRating, rating, timestamp)
      }
    })
    ratingsWithPre.flatMap(_._2)
  }
}



