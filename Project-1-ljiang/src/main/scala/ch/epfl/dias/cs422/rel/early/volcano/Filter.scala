package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

//  protected var curr_ind: Int = 0
//  protected var filter_table: IndexedSeq[Tuple] = IndexedSeq()
//  protected var filter_size: Int = 0
//  protected var new_rle: Option[Tuple] = _

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }
//  override def open(): Unit = {
//    filter_table = filter_table ++ input.toIndexedSeq.filter(r => predicate(r))
//    filter_size = filter_table.size
//  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    val new_tuple = input.next()
    if (new_tuple == NilTuple) {
      // no more input
      return NilTuple
    } else if (predicate(new_tuple.get)) {
      return new_tuple
    }
    next()
  }
//  override def next(): Option[Tuple] = {
//    if (curr_ind < filter_size) {
//      curr_ind += 1
//      Option(filter_table(curr_ind - 1))
//    } else {
//      NilTuple
//    }
//  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close() // add input.close() here
  }
}
