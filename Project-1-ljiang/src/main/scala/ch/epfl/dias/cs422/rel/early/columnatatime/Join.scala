package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  // self-defined parameters
  protected var joined: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  protected var hash_map: Map[Int, IndexedSeq[Tuple]] = _
  protected var right_seq: IndexedSeq[Tuple] = _
  protected var left_seq: IndexedSeq[Tuple] = _

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    // form the map using hashcode
    val left_seq = left.execute().transpose.filter(r => r.last == true).map(r => r.dropRight(1))
    val right_seq = right.execute().transpose.filter(r => r.last == true).map(r => r.dropRight(1))
    if (left_seq.isEmpty || right_seq.isEmpty) {
      IndexedSeq[HomogeneousColumn]()
    } else {
      if (left_seq.size < right_seq.size) {
        hash_map = right_seq.groupBy(t => (for (j <- getRightKeys) yield t(j)).hashCode())
        // foreach row in larger, check if it exists in map
        for (l_t <- left_seq) {
          val row_hash = (for (i <- getLeftKeys) yield l_t(i)).hashCode()
          if (hash_map.contains(row_hash)) {
            joined = joined ++ hash_map(row_hash).map(r => l_t ++ r)
          }
        }
        val num_group = joined.size
        joined = joined.transpose :+ IndexedSeq.fill(num_group)(true)
        joined.map(r => toHomogeneousColumn(r))
      } else {
        hash_map = left_seq.groupBy(t => (for (j <- getLeftKeys) yield t(j)).hashCode())
        // foreach row in larger, check if it exists in map
        for (r_t <- right_seq) {
          val row_hash = (for (i <- getRightKeys) yield r_t(i)).hashCode()
          if (hash_map.contains(row_hash)) {
            joined = joined ++ hash_map(row_hash).map(r => r ++ r_t)
          }
        }
        val num_group = joined.size
        joined = joined.transpose :+ IndexedSeq.fill(num_group)(true)
        joined.map(r => toHomogeneousColumn(r))
      }
    }
  }
}
