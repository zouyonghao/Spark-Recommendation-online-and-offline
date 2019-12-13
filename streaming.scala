import scala.collection.mutable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

val msgConsumerGroup = "recommending-message-consumer-group"
val minSimilarity = 0.7

def getUserRecentRatings(collection: MongoCollection, K: Int, userId: Int, movieId: Int, rate: Double, timestamp: Long): Array[(Int, Double)] = {
    //function feature: 通过MONGODB中评分的时间戳大小来获取userId的最近K-1次评分，与本次评分组成最近K次评分
    //return type：最近K次评分的数组，每一项是<movieId, rate>
    val query = MongoDBObject("userId" -> userId)
    val orderBy = MongoDBObject("timestamp" -> -1)
    val recentRating = collection.find(query).sort(orderBy).limit(K - 1).toArray.map{ item =>
        (item.get("movieId").toString.toInt, item.get("rate").toString.toDouble)
    }.toBuffer
    recentRating += ((movieId, rate))
    //将本次评分写回到MONGODB，为了测试方便暂时不需要，上线再取消注释
    //collection.insert(MongoDBObject("userId" -> userId, "movieId" -> movieId, "rate" -> rate, "timestamp" -> timestamp))
    recentRating.toArray
}

def getSimilarMovies(mostSimilarMovies: collection.Map[Int, Array[Int]], collectionForRatingRecords: MongoCollection, movieId: Int, userId: Int, K: Int): Array[Int] = {
    //function feature: 从广播变量中获取movieId最相似的K个电影，并通过MONGODB来过滤掉已被评分的电影
    //return type：与movieId最相似的电影们，每一项是<other_movieId>
    import com.mongodb.casbah.Imports._
    val similarMoviesBeforeFilter = mostSimilarMovies.getOrElse(movieId, Array[Int]())
    val query = MongoDBObject("userId" -> movieId)
    val condition = "movieId" $in similarMoviesBeforeFilter
    val hasRated = collectionForRatingRecords.find(query ++ condition).toArray.map(_.get("movieId").toString.toInt).toSet
    similarMoviesBeforeFilter.filter(hasRated.contains(_) == false)
}

def getSimilarityBetween2Movies(simHash: collection.Map[Int, collection.Map[Int, Double]], movieId1: Int, movieId2: Int): Double = {
    //function feature: 从广播变量中获取movieId1与movieId2的相似度，不存在、或movieId1=movieId2视为毫无相似，相似度为0
    //return type：movieId1与movieId2的相似度
    val (smallerId, biggerId) = if (movieId1 < movieId2) (movieId1, movieId2) else (movieId2, movieId1)
    if (smallerId == biggerId) {
        return 0.0
    }
    simHash.get(smallerId) match {
        case Some(subSimHash) =>
        subSimHash.get(biggerId) match {
            case Some(sim) => sim
            case None => 0.0
        }
        case None => 0.0
    }
}

def log(m: Double): Double = math.log(m) / math.log(2)

def createUpdatedRatings(simiHash: collection.Map[Int, collection.Map[Int, Double]], recentRatings: Array[(Int, Double)], candidateMovies: Array[Int]): Array[(Int, Double)] = {
    //function feature: 核心算法，计算每个备选电影的预期评分
    //return type：备选电影预计评分的数组，每一项是<movieId, maybe_rate>
    val allSimilars = mutable.ArrayBuffer[(Int, Double)]()

    val increaseCounter = mutable.Map[Int, Int]()
    val reduceCounter = mutable.Map[Int, Int]()

    for (cmovieId <- candidateMovies; (rmovieId, rate) <- recentRatings) {
        val sim = getSimilarityBetween2Movies(simiHash, rmovieId, cmovieId)
        if (sim > minSimilarity) {
        allSimilars += ((cmovieId, sim * rate))
        if (rate >= 3.0) {
            increaseCounter(cmovieId) = increaseCounter.getOrElse(cmovieId, 0) + 1
        } else {
            reduceCounter(cmovieId) = reduceCounter.getOrElse(cmovieId, 0) + 1
        }
        }
    }
    allSimilars.toArray.groupBy{case (movieId, value) => movieId}
        .map{ case (movieId, simArray) =>
        (movieId, simArray.map(_._2).sum / simArray.length + log(increaseCounter.getOrElse[Int](movieId, 1)) - log(reduceCounter.getOrElse[Int](movieId, 1)))
        }.toArray
}

def updateRecommends2MongoDB(collection: MongoCollection, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Boolean = {
    //function feature: 将备选电影的预期评分回写到MONGODB中
    val endTimeMillis = System.currentTimeMillis()
    /*
    val query = MongoDBObject("userId" -> userId)
    val setter = $set("recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"),
    "timedelay" -> (System.currentTimeMillis() - startTimeMillis).toDouble / 1000)
    collection.update(query, setter, upsert = true, multi = false)
    */
    val toInsert = MongoDBObject("userId" -> userId,
        "recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"),
        "timedelay" -> (endTimeMillis - startTimeMillis).toDouble / 1000)
    collection.insert(toInsert)
    true
}