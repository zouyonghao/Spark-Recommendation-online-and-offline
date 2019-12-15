import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import java.net.Socket
import org.apache.log4j.{Level, Logger}

object SimpleApp {

  val minSimilarity = 0.7

  object SocketToRedis extends Serializable {
    val s = new Socket("thumm01", 6379)
    def getRatings(userId: String): Array[(Int, Double)] = {
      val os = s.getOutputStream();
      os.write(("get " + userId + "\r\n").getBytes());
      os.flush();
      Thread.sleep(10);
      val length = s.getInputStream().available()
      val data = new Array[Byte](length + 1)
      s.getInputStream().read(data, 0, length)
      val dataString =
        data
          .slice(data.indexOf('\n'.toByte), length)
          .map(_.toChar)
          .mkString
          .trim
      if (dataString.length > 0) {
        // println(dataString)
        dataString
          .split("\\|")
          .map(i => (i.split(",")(0).toInt, i.split(",")(1).toDouble))
      } else
        Array[(Int, Double)]()
    }

    def setRatings(userId: String, newRecommends: Array[(Int, Double)]) = {
      val content =
        newRecommends
          .map(item => item._1.toString + "," + item._2.toString)
          .mkString("|")
      val os = s.getOutputStream();
      os.write(("set " + userId.toString + " " + content + "\r\n").getBytes());
      os.flush();
      Thread.sleep(10);
      val length = s.getInputStream().available()
      val data = new Array[Byte](length + 1)
      s.getInputStream().read(data, 0, length)

      Logger
        .getLogger(this.getClass())
        .error(data.slice(0, length).map(_.toChar).mkString)
    }
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("streamingRecommend168")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.concurrentJobs", "9")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.cores", "10")

    val sc = new SparkContext(sparkConf)

    val movie2movieList =
      sc.objectFile[(Int, List[(Int, Double)])](
          "offline/allSimilarity_group_and_sorted"
      )

    val topKMostSimilarMovies =
      movie2movieList
        .map(i => (i._1, i._2.take(100).map(l => l._1)))
        .collectAsMap
    val movie2movieSimilarity =
      movie2movieList.map(i => (i._1, i._2.take(100).toMap)).collectAsMap

    val ssc = new StreamingContext(sc, Seconds(5))

    val bTopKMostSimilarMovies =
      ssc.sparkContext.broadcast(topKMostSimilarMovies)
    val bMovie2movieSimilarity =
      ssc.sparkContext.broadcast(movie2movieSimilarity)

    val dataDStream =
      ssc
        .socketTextStream("thumm01", 4321)
        .filter(l => l.split(" ").length == 3)
        .map { l =>
          val array: Array[String] = l.split(" ")
          Logger.getLogger(this.getClass()).error(l)
          val userId = array(0).toInt
          val movieId = array(1).toInt
          val rate = array(2).toDouble
          // val startTimeMillis = array(3).toLong
          // (userId, movieId, rate, startTimeMillis)
          (userId, movieId, rate)
        }

    dataDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd
          .map {
            case (userId, movieId, rate) =>
              //获取近期评分记录
              val recentRatings = getUserRecentRatings(userId, movieId, rate)

              //获取备选电影
              val candidateMovies =
                getSimilarMovies(
                    bTopKMostSimilarMovies.value,
                    movieId,
                    recentRatings
                )

              //为备选电影推测评分结果
              val updatedRecommends =
                createUpdatedRatings(
                    bMovie2movieSimilarity.value,
                    recentRatings,
                    candidateMovies
                )

              SocketToRedis.setRatings(userId.toString, updatedRecommends)
              true
          }
          .count()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getUserRecentRatings(
      userId: Int,
      movieId: Int,
      rate: Double
  ): Array[(Int, Double)] = {
    var recentRatings = SocketToRedis.getRatings(userId.toString())
    recentRatings = recentRatings :+ ((movieId, rate))

    Logger
      .getLogger(this.getClass())
      .error("recentRatings.length: " + recentRatings.length)
    recentRatings
  }

  def getSimilarMovies(
      topKMostSimilarMovies: collection.Map[Int, List[Int]],
      movieId: Int,
      recentRatings: Array[(Int, Double)]
  ): Array[Int] = {
    val candidateMovies = topKMostSimilarMovies
      .getOrElse(movieId, List[Int]())
      .filter(!recentRatings.contains(_))
      .toArray
    Logger
      .getLogger(this.getClass())
      .error("candidateMovies.length: " + candidateMovies.length)
    return candidateMovies
  }

  def createUpdatedRatings(
      movie2movieSimilarity: collection.Map[Int, Map[Int, Double]],
      recentRatings: Array[(Int, Double)],
      candidateMovies: Array[Int]
  ): Array[(Int, Double)] = {
    val allSimilars = mutable.ArrayBuffer[(Int, Double)]()
    val increaseCounter = mutable.Map[Int, Int]()
    val reduceCounter = mutable.Map[Int, Int]()

    for (cmovieId <- candidateMovies; (rmovieId, rate) <- recentRatings) {
      // val sim = getSimilarityBetween2Movies(rmovieId, cmovieId)
      val sim = movie2movieSimilarity
        .getOrElse(rmovieId, Map[Int, Double]())
        .get(cmovieId) match {
        case Some(d) => d
        case None    => 0.0
      }
      if (sim > minSimilarity) {
        allSimilars += ((cmovieId, sim * rate))
        if (rate >= 3.0) {
          increaseCounter(cmovieId) = increaseCounter.getOrElse(cmovieId, 0) + 1
        } else {
          reduceCounter(cmovieId) = reduceCounter.getOrElse(cmovieId, 0) + 1
        }
      }
    }

    allSimilars.toArray
      .groupBy { case (movieId, value) => movieId }
      .map {
        case (movieId, simArray) =>
          (
              movieId,
              simArray.map(_._2).sum / simArray.length + math
                .log(increaseCounter.getOrElse[Int](movieId, 1)) / math
                .log(2) - math
                .log(reduceCounter.getOrElse[Int](movieId, 1)) / math.log(2)
          )
      }
      .toArray
  }
}
