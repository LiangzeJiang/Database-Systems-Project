package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEFilter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

//  protected var curr_ind: Int = 0
//  protected var filter_table: IndexedSeq[RLEentry] = IndexedSeq()
//  protected var filter_size: Int = 0
  /**
    * @inheritdoc
    */
//    override def open(): Unit = {
//      filter_table = filter_table ++ input.toIndexedSeq.filter(r => predicate(r.value))
//      filter_size = filter_table.size
//    }
  override def open(): Unit = {
    input.open()
  }
  /**
    * @inheritdoc
    */
//  override def next(): Option[RLEentry] = {
//    if (curr_ind < filter_size) {
//      curr_ind += 1
//      Option(filter_table(curr_ind - 1))
//    } else {
//      NilRLEentry
//    }
//  }
  override def next(): Option[RLEentry] = {
    val new_tuple = input.next()
    if (new_tuple == NilRLEentry) {
      // no more input
      return NilRLEentry
    } else if (predicate(new_tuple.get.value)) {
      return new_tuple
    }
    next()
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close() // add input.close() here
  }
}
