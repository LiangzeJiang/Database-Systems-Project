package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  // build buckets here
  private val minHash = new MinHash(seed)
  // compute MinHash value using a MinHash object
  private val dataHashValue: RDD[(String, Int)] = minHash.execute(data)
  // use MinHash value to index the data points, organizing them in buckets
  private val buckets = dataHashValue
    .map(r =>(r._2, r._1))
    .groupByKey()
    .mapValues(v => v.toSet).collect().toMap // RDD[(Int, Set[String])]

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute near neighbors here
    val queryHash = minHash.execute(queries)
    val broadcastBuckets = sqlContext.sparkContext.broadcast(buckets)
//    queryHash.mapValues(broadcastBuckets.value(_))
    queryHash.mapValues(broadcastBuckets.value.getOrElse(_, Set[String]())) // getOrElse for no match scenarios

  }
}
