package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.RelCollation


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected var sorted: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  protected var curr: Int = 0
  // self-defined parameters
  protected var unsorted: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  protected var t1_index: Comparable[Any] = _
  protected var t2_index: Comparable[Any] = _
  protected var index: Int = _
  protected var order: Boolean = _
  protected var field_size: Int = collation.getFieldCollations.size()
  protected var field_ind: IndexedSeq[Int] = for (i <- 0 until field_size) yield collation.getFieldCollations.get(i).getFieldIndex
  protected var field_ord: IndexedSeq[Boolean] = for (i <- 0 until field_size) yield collation.getFieldCollations.get(i).direction.isDescending
  protected var sorted_size: Int = 0

  // self-defined functions
  def deconstructCollation(tuple1: Tuple, tuple2: Tuple): Boolean = {
    for (i <- 0 until field_size) {
      index = field_ind(i)
      order = field_ord(i)
      t1_index = tuple1(index).asInstanceOf[Comparable[Any]]
      t2_index = tuple2(index).asInstanceOf[Comparable[Any]]

      if (t1_index.compareTo(t2_index) == 0) {
      } else if (t1_index.compareTo(t2_index) > 0 && order) {
        return true
      } else if (t1_index.compareTo(t2_index) > 0 && !order) {
        return false
      } else if (t1_index.compareTo(t2_index) < 0 && !order) {
        return true
      } else return false
    }
    true
  }


  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    unsorted = input.toIndexedSeq
    if (offset.isDefined && fetch.isDefined ) {
      sorted = unsorted.
        sortWith((t1, t2) => deconstructCollation(t1, t2)).
        slice(offset.get, offset.get + fetch.get)
    } else if (offset.isEmpty && fetch.isDefined) {
      sorted = unsorted.
        sortWith((t1, t2) => deconstructCollation(t1, t2)).
        slice(0, fetch.get)
    } else if (offset.isDefined && fetch.isEmpty) {
      sorted = unsorted.
        sortWith((t1, t2) => deconstructCollation(t1, t2)).
        slice(offset.get, unsorted.size)
    } else {
      sorted = unsorted.
        sortWith((t1, t2) => deconstructCollation(t1, t2))
    }
    sorted_size = sorted.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (curr < sorted_size) {
      curr += 1
      Option(sorted(curr - 1))
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
