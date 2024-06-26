package app.recommender.collaborativeFiltering

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val latestRatings =  ratingsRDD.groupBy{
      case (userId, movieId, _, _, ts) => (userId, movieId)
    }.mapValues(x => x.toList.maxBy(_._5))
      .map { x => Rating(x._1._1, x._2._2, x._2._4) }
    // (userId, movieId, rating)

    // to avoid stackoverflow error

    val als = new ALS().setCheckpointInterval(1)
      .setSeed(seed)
      .setRank(rank)
      .setIterations(maxIterations)
      .setLambda(regularizationParameter)
      .setBlocks(n_parallel)
    model = als.run(latestRatings)
  }

  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }

}
