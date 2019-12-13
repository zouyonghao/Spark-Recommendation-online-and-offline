sc.textFile("offline/allSimilarity_alsAndCos")
    .map(line => {
            val array = line.split(",")
            val movieId1 = array(0).toInt
            val movieId2 = array(1).toInt
            val rate = array(2).toDouble
            (movieId1, (movieId2, rate))
        }
    )
    .groupByKey()
    .map(a => (a._1, a._2.toList.sortBy(_._2).reverse))
    .saveAsObjectFile("offline/allSimilarity_sorted")

sc.objectFile[(Int, List[(Int,Double)])]("offline/allSimilarity_group_and_sorted")
