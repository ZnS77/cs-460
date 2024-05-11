package app.loaders

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val distFile = sc.textFile("./src/main/resources/" + path)
    val results = distFile.map{ line =>
      val fields = line.split("\\|")
      val movieId = fields(0).toInt
      val title = fields(1).replaceAll("^\"|\"$", "")
      val keywords = fields.slice(2, fields.length)
        .map(x=>x.replaceAll("^\"|\"$", ""))
        .toList
      (movieId, title, keywords)
    }
    results.persist(StorageLevel.MEMORY_ONLY)
    results
  }
}

object MoviesLoaderTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MoviesLoaderTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val path = "/movies_small.csv"
      val loader = new MoviesLoader(sc, path)
      val moviesRDD = loader.load()

      println("Movies loaded:")
      moviesRDD.take(3).foreach(println)
    } finally {
      sc.stop()
    }
  }
}
