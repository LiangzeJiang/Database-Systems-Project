package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem}
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {


  protected var sorted: IndexedSeq[IndexedSeq[Any]] = IndexedSeq()
  // self-defined parameters
//  protected var t1_index: Comparable[Any] = _
//  protected var t2_index: Comparable[Any] = _
  protected val field_size: Int = collation.getFieldCollations.size()
  protected val field_ind: IndexedSeq[Int] = for (i <- 0 until field_size) yield collation.getFieldCollations.get(i).getFieldIndex
  protected val field_ord: IndexedSeq[Boolean] = for (i <- 0 until field_size) yield collation.getFieldCollations.get(i).direction.isDescending


  // self-defined functions
  def deconstructCollation(tuple1: IndexedSeq[Any], tuple2: IndexedSeq[Any]): Boolean = {

    for (i <- 0 until field_size) {
      val index = field_ind(i)
      val order = field_ord(i)
      val t1_index = tuple1(index).asInstanceOf[Comparable[Any]]
      val t2_index = tuple2(index).asInstanceOf[Comparable[Any]]

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
  override def execute(): IndexedSeq[Column] = {
    val unsorted = input.toIndexedSeq.transpose.filter(r => r.last==true).map(r => r.dropRight(1))

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
    val num_group = sorted.size
    sorted = sorted.transpose :+ IndexedSeq.fill(num_group)(true)
    sorted
  }

}
