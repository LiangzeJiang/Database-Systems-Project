package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute exact near neighbors here
    // for each query in rdd, compute and compare jaccard with each movie in data, this can be a cross product
    rdd
      .zipWithUniqueId() // use uniqueId to solve duplicate problem
      .map(r => (r._1._1, (r._1._2, r._2)))
      .cartesian(data)
      .filter(r =>jaccard(r._1._2._1.toSet, r._2._2.toSet)>=threshold)
      .map(r =>((r._1._2._2, r._1._1), r._2._1))
      .groupByKey()
      .map(r =>(r._1._2, r._2.toSet))

  }

  def jaccard (a: Set[String], b: Set[String]): Double = {
    if (a.nonEmpty && b.nonEmpty) a.intersect(b).size.toDouble / a.union(b).size.toDouble
    else 0.0
  } // warning: when a or b both are empty...

}
