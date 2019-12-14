import scala.collection.mutable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import java.net.Socket

val minSimilarity = 0.7

val movie2movieList = sc.objectFile[(Int, List[(Int,Double)])]("offline/allSimilarity_group_and_sorted")

val topKMostSimilarMovies = movie2movieList.map(i => (i._1, i._2.take(100).map(l => l._1))).collectAsMap
val movie2movieSimilarity = movie2movieList.map(i => (i._1, i._2.take(100).toMap)).collectAsMap

val ssc = new StreamingContext(sc, Seconds(5))

val bTopKMostSimilarMovies = ssc.sparkContext.broadcast(topKMostSimilarMovies)
val bMovie2movieSimilarity = ssc.sparkContext.broadcast(movie2movieSimilarity)

val dataDStream = ssc.socketTextStream("thumm01", 4321).filter(l => l.split(" ").length == 3).map{ l =>
    val array: Array[String] = l.split(" ")
    val userId = array(0).toInt
    val movieId = array(1).toInt
    val rate = array(2).toDouble
    // val startTimeMillis = array(3).toLong
    // (userId, movieId, rate, startTimeMillis)
    (userId, movieId, rate)
}

dataDStream.foreachRDD(rdd => {
    if (!rdd.isEmpty) {
      rdd.map{ case (userId, movieId, rate) =>
            //获取近期评分记录
            // val recentRatings = getUserRecentRatings(userId, movieId, rate)
            var recentRatings = Array[(Int, Double)]()
            val s = new Socket("localhost", 6379)
            val os = s.getOutputStream();
            os.write(("get " + userId + "\r\n").getBytes());
            os.flush();
            Thread.sleep(10);
            var length = s.getInputStream().available()
            var data = new Array[Byte](length + 1)
            s.getInputStream().read(data, 0, length)
            // s.close()
            val dataString = data.slice(data.indexOf('\n'.toByte), length).map(_.toChar).mkString.trim
            if (dataString.length > 0) {
            // println(dataString)
            recentRatings = dataString.split("\\|").map(i => (i.split(",")(0).toInt, i.split(",")(1).toDouble))
            }
            recentRatings :+ ((movieId, rate))
            //获取备选电影
            // val candidateMovies = getSimilarMovies(movieId, recentRatings)
            // val candidateMovies = bTopKMostSimilarMovies.value.getOrElse(movieId, List[Int]()).filter(!recentRatings.contains(_)).toArray
            //为备选电影推测评分结果
            // val updatedRecommends = createUpdatedRatings(recentRatings, candidateMovies)
            // val allSimilars = mutable.ArrayBuffer[(Int, Double)]()
            // val increaseCounter = mutable.Map[Int, Int]()
            // val reduceCounter = mutable.Map[Int, Int]()

            // for (cmovieId <- candidateMovies; (rmovieId, rate) <- recentRatings) {
            //     // val sim = getSimilarityBetween2Movies(rmovieId, cmovieId)
            //     val sim = bMovie2movieSimilarity.value.getOrElse(rmovieId, Map[Int, Double]()).get(cmovieId) match {
            //         case Some(d) => d
            //         case None => 0.0
            //     }
            //     if (sim > minSimilarity) {
            //         allSimilars += ((cmovieId, sim * rate))
            //         if (rate >= 3.0) {
            //             increaseCounter(cmovieId) = increaseCounter.getOrElse(cmovieId, 0) + 1
            //         } else {
            //             reduceCounter(cmovieId) = reduceCounter.getOrElse(cmovieId, 0) + 1
            //         }
            //     }
            // }
            // val updatedRecommends = allSimilars.toArray.groupBy{case (movieId, value) => movieId}.map{ 
            //     case (movieId, simArray) =>
            //     (movieId, simArray.map(_._2).sum / simArray.length + math.log(increaseCounter.getOrElse[Int](movieId, 1))/ math.log(2) - math.log(reduceCounter.getOrElse[Int](movieId, 1))/ math.log(2))
            // }.toArray

            // // updateRecommends2Redis(updatedRecommends, userId)
            // val content = updatedRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|")
            // os.write(("set " + userId.toString + " " + content + "\r\n").getBytes());
            // os.flush();
            // // Thread.sleep(10);
            // length = s.getInputStream().available()
            // data = new Array[Byte](length + 1)
            // s.getInputStream().read(data, 0, length)
            // s.close()
            true
        }.count()
    }
})

// val updateFunc = (values: Seq[Int], state: Option[Int]) => {
//     val currentCount = values.sum
//     val previousCount = state.getOrElse(0)
//     Some(currentCount + previousCount)
// }

// val movieIdCount = dataDStream.map{case (userId, movieId, rate) => (movieId, 1)}
// val stateDStream = movieIdCount.updateStateByKey[Int](updateFunc)

// //选出TOP5的电影
// stateDStream.foreachRDD{rdd =>
//     val hotMovies = rdd.top(5)(Ordering.by[(Int, Int), Int](_._2))
//     for ((movieId, counter) <- hotMovies) {
//         println(movieId + ":" + counter)
//     }
// }

ssc.checkpoint("testStream/checkpoint_dir")
ssc.start()

// ssc.awaitTermination()
