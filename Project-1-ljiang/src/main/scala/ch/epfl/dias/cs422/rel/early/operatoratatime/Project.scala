package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  /**
   * @inheritdoc
   */
    def execute(): IndexedSeq[Column] = {
      val row_input = input.toIndexedSeq.transpose
      row_input.map(r => evaluator(r.dropRight(1)) :+ r.last
      ).transpose
    }
//  def execute(): IndexedSeq[Column] = {
//    val row_input = input.toIndexedSeq.transpose
//    row_input.map(r => {
//      if (r.last==true) {
//        evaluator(r.dropRight(1)) :+ true
//      } else {
//        evaluator(r.dropRight(1)) :+ false
//      }
//    }).transpose
//  }
}
