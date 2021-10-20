package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

//  protected var curr: Int = 0
//  protected var proj_size: Int = 0
//  protected var projected: IndexedSeq[Tuple] = IndexedSeq()
  /**
    * @inheritdoc
    */
  override def open(): Unit = input.open()
//  override def open(): Unit = {
//    projected = projected ++ input.toIndexedSeq.map(r => evaluator(r))
//    proj_size = projected.size
//  }
  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    val new_tuple = input.next()
    if (new_tuple == NilTuple) {
      // encounter the last input
      NilTuple
    } else {
      // meaningful input
      Option(evaluator(new_tuple.get))
    }
  }
//  override def next(): Option[Tuple] = {
//    if (curr < proj_size) {
//      curr += 1
//      Option(projected(curr - 1))
//    } else {
//      NilTuple
//    }
//  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
