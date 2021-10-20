package lsh
import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute AND construction results here
    // Note that Construction.eval() returns RDD[(String, Set[String])]
    val multiRes = children.map(c => c.eval(rdd).sortByKey().zipWithIndex())
    val entireRes = multiRes.reduce(_ ++ _).map(r =>(r._2, r._1))

    val andRes = entireRes
      .groupByKey()
      .values
      .map(v =>v.reduce((a,b) =>(a._1, a._2.intersect(b._2))))
    andRes
  }
}
