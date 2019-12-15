import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

// Load and parse the data
val data = sc.textFile("ratings.csv")
val ratings = data.map(_.split(',') match { case Array(user, movieId, rating, timestamp) =>
  Rating(user.toInt, movieId.toInt, rating.toDouble)
})

// Build the recommendation model using ALS
val rank = 10
val numIterations = 10
val model = ALS.train(ratings, rank, numIterations, 0.01)

// Evaluate the model on rating data
val userMovie = ratings.map { case Rating(user, movieId, rate) =>
  (user, movieId)
}
val predictions =
  model.predict(userMovie).map { case Rating(user, movieId, rate) =>
    ((user, movieId), rate)
  }
val ratesAndPreds = ratings.map { case Rating(user, movieId, rate) =>
  ((user, movieId), rate)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((user, movieId), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean()
println(s"Mean Squared Error = $MSE")

// Save and load model
model.save(sc, "myCollaborativeFilter")
// val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")