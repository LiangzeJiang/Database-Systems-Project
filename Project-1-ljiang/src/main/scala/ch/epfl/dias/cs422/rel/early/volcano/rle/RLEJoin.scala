package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {


  // self-defined parameters
  protected var joined: IndexedSeq[RLEentry] = IndexedSeq()
  protected var curr: Int = 0
  protected var joined_size: Int = 0


  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    right.open()
    val hash_map = left.groupBy(t => (for (j <- getLeftKeys) yield t.value(j)).hashCode())
    var r_t = right.next()
    var head = 0
    while(r_t.isDefined) {
      val row_hash = (for (i <- getRightKeys) yield r_t.get.value(i)).hashCode()
      if (hash_map.contains(row_hash)) {
        joined = joined ++ hash_map(row_hash).map(r => { head = head + r_t.get.length.toInt; RLEentry(head - r_t.get.length.toInt, r_t.get.length, r.value ++ r_t.get.value)})
      }
      r_t = right.next()
    }
    joined_size = joined.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (curr < joined_size) {
      curr += 1
      Option(joined(curr - 1))
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
//    left.close()
//    right.close()
  }
}
