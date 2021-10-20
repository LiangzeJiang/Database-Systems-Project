package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 43)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 44)
    new ANDConstruction(List(lsh1, lsh2, lsh3))
  }

  def construction2(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 1)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 6)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 18)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 45)
    new ORConstruction(List(lsh1, lsh2, lsh3, lsh4))
  }

  def dist_measure(res: RDD[(String, Set[String])], query: RDD[(String, List[String])], corpus: RDD[(String, List[String])], exact: ExactNN): Double = {
    // function that compute the average distance of query and its neighbors
    res
      .zipWithUniqueId()
      .map(r =>((r._2, r._1._1), r._1._2))
      .flatMapValues(_.toSeq)
      .map(r =>(r._1._2, (r._1._1, r._2)))
      .join(query)
      .map(r => (r._2._1._2, (r._2._1._1, r._1, r._2._2)))
      .join(corpus)
      .map(r =>(r._2._1._1, exact.jaccard(r._2._1._3.toSet, r._2._2.toSet)))
      .groupByKey()
      .map(r => r._2.sum / r._2.size.toDouble)
      .mean()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //type your queries here
    val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .sample(false, 0.05)

    // construct
    val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lshBase =  new BaseConstruction(sqlContext, rdd_corpus, 42)
    val lshBalanced =  new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, 8)
    val lshBroad =  new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
    val lshAnd = new ANDConstruction(List(lshBase, new BaseConstruction(sqlContext, rdd_corpus, 43)))
    val lshOr = new ORConstruction(List(lshBase, new BaseConstruction(sqlContext, rdd_corpus, 43)))

    // evaluate
    val ground = exact.eval(rdd_query)
    val resBase = lshBase.eval(rdd_query)
    val resBalanced = lshBalanced.eval(rdd_query)
    val resBroad = lshBroad.eval(rdd_query)
    val resAnd = lshAnd.eval(rdd_query)
    val resOr = lshOr.eval(rdd_query)

    // measure the time
    val tGround = System.nanoTime
    ground.count()
    val durGround = (System.nanoTime - tGround) / 1e9d

    val tBase = System.nanoTime
    resBase.count()
    val durBase = (System.nanoTime - tBase) / 1e9d

    val tBalanced = System.nanoTime
    resBalanced.count()
    val durBalanced = (System.nanoTime - tBalanced) / 1e9d

    val tBroad = System.nanoTime
    resBroad.count()
    val durBroad = (System.nanoTime - tBroad) / 1e9d

    val tAnd = System.nanoTime
    resAnd.count()
    val durAnd = (System.nanoTime - tAnd) / 1e9d

    val tOr = System.nanoTime
    resOr.count()
    val durOr = (System.nanoTime - tOr) / 1e9d

    // measure the accuracy
    val recallBase = recall(ground, resBase); val precBase = precision(ground, resBase)
    val recallBalanced = recall(ground, resBalanced); val precBalanced = precision(ground, resBalanced)
    val recallBroad = recall(ground, resBroad); val precBroad = precision(ground, resBroad)
    val recallAnd = recall(ground, resAnd); val precAnd = precision(ground, resAnd)
    val recallOr = recall(ground, resOr); val precOr = precision(ground, resOr)

    // measure the distance
    val uniqueCorpus = rdd_corpus.distinct().cache()
    val uniqueQuery = rdd_query.distinct().cache()

    val distGround = dist_measure(ground, uniqueQuery, uniqueCorpus, exact)
    val distBase = dist_measure(resBase, uniqueQuery, uniqueCorpus, exact)
    val distBalanced = dist_measure(resBalanced, uniqueQuery, uniqueCorpus, exact)
    val distBroad = dist_measure(resBroad, uniqueQuery, uniqueCorpus, exact)
    val distAnd = dist_measure(resAnd, uniqueQuery, uniqueCorpus, exact)
    val distOr = dist_measure(resOr, uniqueQuery, uniqueCorpus, exact)


    // compute the recall and precision
    println(query_file)
    println("=====================recall and precision========================")
    println("BaseConstruction accuracy: recall = " + recallBase + ", precision = " + precBase)
    println("BaseConstructionBalanced accuracy: recall = " + recallBalanced + ", precision = " + precBalanced)
    println("BaseConstructionBroadcast accuracy: recall = " + recallBroad + ", precision = " + precBroad)
    println("AndConstruction accuracy: recall = " + recallAnd + ", precision = " + precAnd)
    println("OrConstruction accuracy: recall = " + recallOr + ", precision = " + precOr)

    // output time measurements
    println("=====================time measurements========================")
    println("ExactNN time: " + durGround)
    println("BaseConstruction time: " + durBase)
    println("BaseConstructionBalanced time: " + durBalanced)
    println("BaseConstructionBroadcast time: " + durBroad)
    println("AndConstruction time: " + durAnd)
    println("OrConstructionBroadcast time: " + durOr)

    // output average distance
    println("=====================average distance========================")
    println("ExactNN average distance: " + distGround)
    println("BaseConstruction average distance: " + distBase)
    println("BaseConstructionBalanced average distance: " + distBalanced)
    println("BaseConstructionBroadcast average distance: " + distBroad)
    println("AndConstruction average distance: " + distAnd)
    println("OrConstruction average distance: " + distOr)

  }
}
