package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {


  // self-defined parameters
  protected var aggregated: IndexedSeq[IndexedSeq[Any]] = IndexedSeq()
  protected var curr_tuple: IndexedSeq[Tuple] = _
  protected var curr_key_tuple: Map[IndexedSeq[Any], IndexedSeq[Tuple]] = _
  protected var curr_index: IndexedSeq[RelOperator.Elem] = _
  protected var curr: Int = 0
  protected var curr_group: IndexedSeq[Any] = IndexedSeq()
  protected var aggregated_size: Int = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    curr_tuple = input.toIndexedSeq
    // Get the aggregated result based on four combinations of the emptiness of input and groupSet
    if (curr_tuple.isEmpty) {
      // If input is empty, just return the aggEmptyValue for each agg
      aggregated = IndexedSeq(for (agg <- aggCalls) yield agg.emptyValue)

    } else if (curr_tuple.nonEmpty && groupSet.isEmpty) {
      // If input is not empty and the group by clause is empty, it means no group key.
      aggregated = IndexedSeq(for (agg <- aggCalls) yield curr_tuple.init.foldLeft(agg.getArgument(curr_tuple.last))((i, c) => agg.reduce(i, agg.getArgument(c))))

    } else if (curr_tuple.nonEmpty && !groupSet.isEmpty) {
      // If neither input and group is not empty, then depends on the aggregate calls
      curr_index = for (i <- 0 until groupSet.size() if groupSet.get(i)) yield i // Vector(0,1)
      curr_key_tuple = curr_tuple.groupBy(t => {
        for (j <- 0 until curr_index.size) yield t(j)
      })
      for ((k, v) <- curr_key_tuple) {
        curr_group = (k ++ (for (agg <- aggCalls) yield v.init.foldLeft(agg.getArgument(v.last))((a, b) => agg.reduce(a, agg.getArgument(b)))))
        aggregated = aggregated :+ curr_group
      }
    }
    aggregated_size = aggregated.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (curr < aggregated_size) {
      curr += 1
      Option(aggregated(curr - 1))
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }

}
