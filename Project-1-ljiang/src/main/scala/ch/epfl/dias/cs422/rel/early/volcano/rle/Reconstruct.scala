package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Reconstruct[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

//  protected var l_rle: RLEentry = _
//  protected var r_rle: RLEentry = _
  protected var l_rle: Option[RLEentry] = _
  protected var r_rle: Option[RLEentry] = _
  protected var curr_ind: Int = 0
  protected var l_ents: IndexedSeq[RLEentry] = _
  protected var r_ents: IndexedSeq[RLEentry] = _
  protected var recons_table: IndexedSeq[RLEentry] = IndexedSeq()
  protected var recons_size: Int = 0

  /**
    * @inheritdoc
    */
//  override def open(): Unit = {
//    l_ents = left.toIndexedSeq
//    r_ents = right.toIndexedSeq
//    var l_ind = 0
//    var r_ind = 0
//    var flag = false
//    if (l_ents.nonEmpty && r_ents.nonEmpty) {
//      if(l_ents(0).startVID==r_ents(0).startVID && (l_ents.size == r_ents.size) && (l_ents.forall(r => r.length == 1)) && (r_ents.forall(r => r.length == 1))) flag = true
//    }
//    if (flag) {
//      recons_table = recons_table ++ (for (i <- l_ents.indices) yield RLEentry(i, 1, l_ents(i).value ++ r_ents(i).value))
//    } else {
//        while (l_ind < l_ents.size && r_ind < r_ents.size) {
//
//          l_rle = l_ents(l_ind); r_rle = r_ents(r_ind)
//
//          val l_s = l_rle.startVID; val l_e = l_s + l_rle.length.toInt
//          val r_s = r_rle.startVID; val r_e = r_s + r_rle.length.toInt
//          val overlap = (l_s until l_e).intersect(r_s until r_e)
//          if (overlap.nonEmpty) {
//            recons_table = recons_table :+ RLEentry(overlap.head, overlap.size.toLong, l_rle.value ++ r_rle.value)
//            if (l_e > r_e) {
//              r_ind += 1
//            }
//            else if (l_e == r_e) {
//              l_ind += 1
//              r_ind += 1
//            }
//            else {
//              l_ind += 1
//            }
//          } else {
//            if (l_e - 1 < r_s) {
//              l_ind += 1
//            } else if (r_e - 1 < l_s) {
//              r_ind += 1
//            }
//          }
//        }
//    }
//    recons_size = recons_table.size
//  }
  override def open(): Unit = {
    left.open()
    right.open()
    l_rle = left.next()
    r_rle = right.next()
  }
  /**
    * @inheritdoc
    */
//  override def next(): Option[RLEentry] = {
//    if (curr_ind < recons_size) {
//      curr_ind += 1
//      Option(recons_table(curr_ind - 1))
//    } else {
//      NilRLEentry
//    }
//  }
  override def next(): Option[RLEentry] = {
    while (l_rle.isDefined && r_rle.isDefined) {
      val l_s = l_rle.get.startVID; val l_e = l_s + l_rle.get.length.toInt
      val r_s = r_rle.get.startVID; val r_e = r_s + r_rle.get.length.toInt
      val overlap = (l_s until l_e).intersect(r_s until r_e)
      if (overlap.nonEmpty) {
        val new_ent = Option(RLEentry(overlap.head, overlap.size.toLong, l_rle.get.value ++ r_rle.get.value))
        if (l_e > r_e) {
          r_rle = right.next()
        }
        else if (l_e == r_e) {
          l_rle = left.next()
          r_rle = right.next()
        }
        else {
          l_rle = left.next()
        }
        return new_ent
      } else {
        if (l_e - 1 < r_s) {
          l_rle = left.next()
        } else if (r_e - 1 < l_s) {
          r_rle = right.next()
        }
      }
    }
    NilRLEentry
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
