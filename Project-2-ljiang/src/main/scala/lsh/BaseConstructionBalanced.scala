package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  // build buckets here
  private val minHash = new MinHash(seed)
  // compute MinHash value using a MinHash object
  private val dataHashValue: RDD[(String, Int)] = minHash.execute(data)
  // use MinHash value to index the data points, organizing them in buckets
  private val buckets = dataHashValue
    .map(r =>(r._2, r._1))
    .groupByKey()
    .mapValues(v => v.toSet) // RDD[(Int, Set[String])]

  // (i) compute the number of occurrences of each MinHash value in the n query points,
  // (ii) compute an equi-depth histogram; with p for the number of occurrences, the equi-depth histogram partitions the MinHash value range into sub-ranges with ceil(n/p) elements each,
  // (iii) shuffle query points and buckets such that exactly one partition is assigned to each task,
  // (iv) compute the approximate near-neighbors within each specific partition.
  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    //compute histogram for target buckets of queries
    queries.map(r => (r._2, 1)).reduceByKey(_+_).sortByKey().collect()
  }

  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    // compute the boundaries of bucket partitions
    val ceilnp = math.ceil(histogram.map(_._2).sum.toDouble / partitions.toDouble)
    var boundary = Array[Int]() :+ 0
    var curr_size = 0
    for (i <- histogram) {
      curr_size = curr_size + i._2
      if (curr_size >= ceilnp) {curr_size = 0; boundary = boundary :+ i._1}
    }
    boundary
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute near neighbors with load balancing here
    val queryHash = minHash.execute(queries) // RDD[String, Int]
    val queryhistogram = computeMinHashHistogram(queryHash) // Array[(Int, Int)]
    val partitionBoundary = computePartitions(queryhistogram) // Array[Int]

    // Partition the buckets and queries by boundary
    val partitionedBuckets = buckets.groupBy(r => partitionBoundary.indexWhere(t => r._1 > t))
    val partitionedQueries = queryHash.groupBy(r => partitionBoundary.indexWhere(t => r._2 > t))

    // perform join on subset of queries and buckets
    val shuffledPair = partitionedQueries.join(partitionedBuckets)
    shuffledPair
      .values
      .flatMap(r => {  //Iterable[(String, Int)], Iterable[(Int, Set[String])]
        r._1.map(t => (t._1, r._2.find(k => k._1 == t._2).getOrElse((1, Set[String]()))._2)) // we may cannot find matching minhash, use getOrElse to deal with
      })

  }
}