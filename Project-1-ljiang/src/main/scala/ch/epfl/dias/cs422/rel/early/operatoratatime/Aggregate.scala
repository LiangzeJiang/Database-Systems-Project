package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected var aggregated: IndexedSeq[IndexedSeq[Any]] = IndexedSeq()
  protected var curr_index: IndexedSeq[Any] = _
  protected var curr_key_tuple: Map[IndexedSeq[Any], IndexedSeq[IndexedSeq[Any]]] = _
  protected var curr_group: IndexedSeq[Any] = IndexedSeq()
  protected var num_group: Int = 0
  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    val column_input = input.toIndexedSeq
    val active_table = column_input.transpose.filter(r => r.last==true).map(r => r.dropRight(1))
    if (active_table.isEmpty) {

      aggregated = IndexedSeq(for (agg <- aggCalls) yield agg.emptyValue)
      aggregated = aggregated.transpose :+ IndexedSeq.fill(1)(true)

    } else if (active_table.nonEmpty && groupSet.isEmpty) {

      aggregated = IndexedSeq(for (agg <- aggCalls) yield active_table.init.foldLeft(agg.getArgument(active_table.last))((i, c) => agg.reduce(i, agg.getArgument(c))))
      aggregated = aggregated.transpose :+ IndexedSeq.fill(1)(true)

    } else if (active_table.nonEmpty && !groupSet.isEmpty) {

      curr_index = for (i <- 0 until groupSet.size() if groupSet.get(i)) yield i
      curr_key_tuple = active_table.groupBy(t => {
        for (j <- 0 until curr_index.size) yield t(j)
      })
      for ((k, v) <- curr_key_tuple) {
        curr_group = (k ++ (for (agg <- aggCalls) yield v.init.foldLeft(agg.getArgument(v.last))((a, b) => agg.reduce(a, agg.getArgument(b)))))
        aggregated = aggregated :+ curr_group
        num_group = num_group + 1
      }
      aggregated = aggregated.transpose :+ IndexedSeq.fill(num_group)(true)

    }
    aggregated
  }
}
