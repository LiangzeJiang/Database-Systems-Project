package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {


  // self-defined parameters
  protected var aggregated: IndexedSeq[RLEentry] = IndexedSeq()
  protected var curr_tuple: IndexedSeq[RLEentry] = _
  protected var curr_key_tuple: Map[IndexedSeq[Any], IndexedSeq[RLEentry]] = _
  protected var curr: Int = 0
  protected var curr_group: RLEentry = _
  protected var aggregated_size: Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    curr_tuple = input.toIndexedSeq
    // Get the aggregated result based on four combinations of the emptiness of input and groupSet
    if (curr_tuple.isEmpty) {
      // If input is empty, just return the aggEmptyValue for each agg
      aggregated = aggregated :+ RLEentry(0, 1, for (agg <- aggCalls) yield agg.emptyValue)

    } else if (curr_tuple.nonEmpty && groupSet.isEmpty) {
      // If input is not empty and the group by clause is empty, it means no group key.
      aggregated = aggregated :+ RLEentry(0, 1, for (agg <- aggCalls) yield curr_tuple.init.foldLeft(agg.getArgument(curr_tuple.last.value, curr_tuple.last.length))
      ((i, c) => agg.reduce(i, agg.getArgument(c.value, c.length))))

    } else if (curr_tuple.nonEmpty && !groupSet.isEmpty) {
      // If neither input and group is not empty, then depends on the aggregate calls
      val curr_index = for (i <- 0 until groupSet.size() if groupSet.get(i)) yield i // Vector(0,1)
      curr_key_tuple = curr_tuple.groupBy(t => {
        for (j <- 0 until curr_index.size) yield t.value(j)
      })
      var head = 0
      for ((k, v) <- curr_key_tuple) {
        curr_group = RLEentry(head, 1, k ++ (for (agg <- aggCalls) yield v.init.foldLeft(agg.getArgument(v.last.value, v.last.length))
        ((a, b) => agg.reduce(a, agg.getArgument(b.value, b.length)))))
        aggregated = aggregated :+ curr_group
        head = head + 1
      }
    }
    aggregated_size = aggregated.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (curr < aggregated_size) {
      curr += 1
      Option(aggregated(curr - 1))
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
}
}
