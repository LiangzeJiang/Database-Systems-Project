package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  // build buckets here
  private val minHash = new MinHash(seed)
  // compute MinHash value using a MinHash object
  private val dataHashValue: RDD[(String, Int)] = minHash.execute(data)
  // use MinHash value to index the data points, organizing them in buckets
  private val buckets = dataHashValue
    .map(r =>(r._2, r._1))
    .groupByKey()
    .mapValues(v => v.toSet)

  // sanity check
  // buckets.mapValues(_.size).sortBy(-_._2).take(5).foreach(println)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute near neighbors here(it's the basic LSH)
    val queryHash = minHash.execute(queries)
      .map(r =>(r._2, r._1))
      .leftOuterJoin(buckets) // use left outer join to avoid no match scenario
      .values
      .mapValues(s => s.getOrElse(Set[String]()))

    queryHash
  }
}
