package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // self-defined parameters
  protected var joined: List[Tuple] = List[Tuple]()
  protected var curr: Int = 0

  protected var hash_map: Map[Int, IndexedSeq[Tuple]] = _
  protected var right_seq: IndexedSeq[Tuple] = _
  protected var left_seq: IndexedSeq[Tuple] = _
  protected var joined_size: Int = 0

  /**
    * @inheritdoc
    */

  override def open(): Unit = {
    // form the map using hashcode
    right_seq = right.toIndexedSeq
    left_seq = left.toIndexedSeq
    if (left_seq.size < right_seq.size) {
      hash_map = right_seq.groupBy(t => (for (j <- getRightKeys) yield t(j)).hashCode())
      // foreach row in larger, check if it exists in map
      for (l_t <- left_seq) {
        val row_hash = (for (i <- getLeftKeys) yield l_t(i)).hashCode()
        if (hash_map.contains(row_hash)) {
          joined = joined ++ hash_map(row_hash).map(r => l_t ++ r)
        }
      }
    } else {
      hash_map = left_seq.groupBy(t => (for (j <- getLeftKeys) yield t(j)).hashCode())
      // foreach row in larger, check if it exists in map
      for (r_t <- right_seq) {
        val row_hash = (for (i <- getRightKeys) yield r_t(i)).hashCode()
        if (hash_map.contains(row_hash)) {
          joined = joined ++ hash_map(row_hash).map(r => r ++ r_t)
        }
      }
    }
    joined_size = joined.size
  }

//  override def open(): Unit = {
//    // form the map using hashcode
//    hash_map = left.toIndexedSeq.groupBy(t => (for (j <- getLeftKeys) yield t(j)).hashCode())
//    right_seq = right.toIndexedSeq
//    // foreach row in larger, check if it exists in map
//    for (r_t <- right_seq) {
//      val row_hash = (for (i <- getRightKeys) yield r_t(i)).hashCode()
//      if (hash_map.contains(row_hash)) {
//        joined = joined ++ hash_map(row_hash).map(r => r ++ r_t)
//      }
//    }
//    joined_size = joined.size
//  }

//  override def open(): Unit = {
//    // ideally, use smaller to form a map
//    smaller_key = if (left.size > right.size) {
//      smaller_seq = right.toIndexedSeq
//      larger = left.iterator
//      larger_key = getLeftKeys
//      flag = 0
//      getRightKeys
//    } else {
//      smaller_seq = left.toIndexedSeq
//      larger = right.iterator
//      larger_key = getRightKeys
//      flag = 1
//      getLeftKeys
//    }
//    // form the map using hashcode
//    hash_map = smaller_seq.groupBy(t => {
//      (for (j <- smaller_key) yield t(j)).hashCode()
//    })
//    // foreach row in larger, check if it exists in map
//    while(larger.hasNext) {
//      larger_tuple = larger.next()
//      val row_hash = (for (i <- larger_key) yield larger_tuple(i)).hashCode()
//      if (hash_map.contains(row_hash)) {
//        if (flag == 0) {
//          joined = joined ++ hash_map(row_hash).map(r => larger_tuple ++ r)
//        } else {
//          joined = joined ++ hash_map(row_hash).map(r => r ++ larger_tuple)
//        }
//      }
//    }
//  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (curr < joined_size) {
      curr += 1
      Option(joined(curr - 1))
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
