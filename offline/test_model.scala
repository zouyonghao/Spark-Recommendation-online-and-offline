:load als.scala

val recommandFor1 = model.recommendProductsForUsers(1)

recommandFor1.sortBy(_._2.head.rating, false).take(10)
