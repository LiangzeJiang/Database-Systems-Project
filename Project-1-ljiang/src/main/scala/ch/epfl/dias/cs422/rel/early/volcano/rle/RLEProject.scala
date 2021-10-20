package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  *
  * Note that this in an RLE operator, so it receives [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]] and
  * produces [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]]
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEProject protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

//  protected var curr: Int = 0
//  protected var proj_size: Int = 0
//  protected var projected: IndexedSeq[RLEentry] = IndexedSeq()
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }
//  override def open(): Unit = {
//    projected = projected ++ input.toIndexedSeq.map(rle => RLEentry(rle.startVID,rle.length,evaluator(rle.value)))
//    proj_size = projected.size
//  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    val new_tuple = input.next()
    if (new_tuple == NilRLEentry) {
      // encounter the last input
      NilRLEentry
    } else {
      // meaningful input
      val rle = new_tuple.get
      Option(RLEentry(rle.startVID,rle.length,evaluator(rle.value)))
    }
  }
//  override def next(): Option[RLEentry] = {
//    if (curr < proj_size) {
//      curr += 1
//      Option(projected(curr - 1))
//    } else {
//      NilRLEentry
//    }
//  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
