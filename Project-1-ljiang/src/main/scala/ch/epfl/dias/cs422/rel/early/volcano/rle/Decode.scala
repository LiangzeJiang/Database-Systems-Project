package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

//  protected var decode_table: IndexedSeq[Tuple] = IndexedSeq()
//  protected var decode_size: Int = 0
  protected var curr_ind: Int = 0
  protected var glob_count: Int = 0
  protected var new_rle: Option[RLEentry] = _
  /**
    * @inheritdoc
    */
//  override def open(): Unit = {
//    val input_rle = input.toIndexedSeq
//    for (rle <- input_rle) {
//      decode_table = decode_table ++ (for (_ <- 0 until rle.length.toInt) yield rle.value)
//    }
//    decode_size = decode_table.size
//  }
  override def open(): Unit = {
    input.open()
    new_rle = input.next()
    if (new_rle.isDefined) {
      glob_count = new_rle.get.length.toInt
    }
  }


  /**
    * @inheritdoc
    */
//  override def next(): Option[Tuple] = {
//    if (curr_ind < decode_size) {
//      curr_ind += 1
//      Option(decode_table(curr_ind - 1))
//    } else {
//      NilTuple
//    }
//  }
  override def next(): Option[Tuple] = {
    if (new_rle.isDefined) {
      if (glob_count > 0) {
        glob_count -= 1
        Option(new_rle.get.value)
      } else {
        new_rle = input.next()
        if (new_rle.isDefined) glob_count = new_rle.get.length.toInt
        next()
      }
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
