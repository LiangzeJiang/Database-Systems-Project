package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
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
  protected var joined_size: Int = 0
//  protected var r_t: Option[Tuple] = _
//  protected var glob_count: Int = 0
//protected var hash_map: Map[Int, Iterable[Tuple]] = _
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    right.open()
    val hash_map = left.groupBy(t => (for (j <- getLeftKeys) yield t(j)).hashCode())
    var r_t = right.next()
    while(r_t.isDefined) {
      val row_hash = (for (i <- getRightKeys) yield r_t.get(i)).hashCode()
      if (hash_map.contains(row_hash)) {
        joined = joined ++ hash_map(row_hash).map(r => r ++ r_t.get)
      }
      r_t = right.next()
    }
    joined_size = joined.size
  }

//  override def open(): Unit = {
//    right.open()
//    hash_map = left.groupBy(t => (for (j <- getLeftKeys) yield t(j)).hashCode())
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
//  override def next(): Option[Tuple] = {
//    r_t = right.next()
//    if (r_t.isDefined) {
//      val row_hash = (for (i <- getRightKeys) yield r_t.get(i)).hashCode()
//      if (hash_map.contains(row_hash)) {
//        glob_count = hash_map(row_hash).size
//        hash_map(row_hash).map(r => r ++ r_t.get)
//      }
//    } else {
//      NilTuple
//    }
//  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
//    left.close()
//    right.close()
  }
}

