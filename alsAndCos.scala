import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.sqrt

def cosineSimilarity(vector1: DenseMatrix, vector2: DenseMatrix): Double = vector1.multiply(vector2.transpose).values(0) / (Vectors.norm(Vectors.dense(vector1.values), 2) * Vectors.norm(Vectors.dense(vector2.values), 2))

def calculateAllCosineSimilarity(model: MatrixFactorizationModel, dataDir: String, dateStr: String): Unit = {
    //calculate all the similarity and store the stuff whose sim > 0.5 to Redis.
    val productsVectorRdd = model.productFeatures
        .map{case (movieId, factor) =>
        val factorVector = new DenseMatrix(factor.length, 1, factor)
        (movieId, factorVector)
    }

    val productsSimilarity = productsVectorRdd.cartesian(productsVectorRdd)
        .filter{ case ((movieId1, vector1), (movieId2, vector2)) => movieId1 != movieId2 }
        .map{case ((movieId1, vector1), (movieId2, vector2)) =>
        val sim = cosineSimilarity(vector1, vector2)
        (movieId1, movieId2, sim)
        }.filter(_._3 >= minSimilarity)

        productsSimilarity.map{ case (movieId1, movieId2, sim) => 
            movieId1.toString + "," + movieId2.toString + "," + sim.toString
        }.saveAsTextFile(dataDir + "allSimilarity_" + dateStr)

        productsVectorRdd.unpersist()
        productsSimilarity.unpersist()
}

// Load and parse the data
val data = sc.textFile("ratings.csv")
val ratings = data.map(_.split(',') match { case Array(user, movieId, rating, timestamp) =>
  Rating(user.toInt, movieId.toInt, rating.toDouble)
})

// Build the recommendation model using ALS
val rank = 10
val numIterations = 10
val model = ALS.train(ratings, rank, numIterations, 0.01)

val dataDir = "offline/"
val dataStr = "alsAndCos"

calculateAllCosineSimilarity(model, dataDir, dateStr) //save cos sim.
model.save(sc, dataDir + "ALSmodel_" + dateStr) //save model.

// val realRatings = sc.textFile(dataDir + "realRatings.txt").map{ line =>
//     val lineAttrs = line.trim.split(",")
//     Rating(lineAttrs(1).toInt, lineAttrs(0).toInt, lineAttrs(2).toDouble)
// }

// val rmse = computeRmse(model, realRatings)
// println("the Rmse = " + rmse)