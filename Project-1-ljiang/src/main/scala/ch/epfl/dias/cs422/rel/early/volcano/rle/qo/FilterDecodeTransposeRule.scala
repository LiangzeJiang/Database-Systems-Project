package ch.epfl.dias.cs422.rel.early.volcano.rle.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalDecode
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.logical.LogicalFilter

/**
  * RelRule (optimization rule) that finds a filter above a decode
  * and pushes it bellow it.
  *
  * To use this rule: FilterDecodeTransposeRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class FilterDecodeTransposeRule protected (config: RelRule.Config)
    extends RelRule(config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: LogicalFilter = call.rel(0) // Retrieves the ordinalth matched relational expression
    val decode: LogicalDecode = call.rel(1)

    val newfilter = filter.copy(filter.getTraitSet, decode.getInputs)
    val newdecode = decode.copy(newfilter)
    call.transformTo(
      newdecode
    )
  }
}

object FilterDecodeTransposeRule {

  /**
    * Configuration for a [[FilterDecodeTransposeRule]]
    */
  val INSTANCE = new FilterDecodeTransposeRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
    // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalFilter]
        b.operand(classOf[LogicalFilter])
          // that has inputs:
          .oneInput(b1 =>
            // A node that is a LogicalDecode
            b1.operand(classOf[LogicalDecode])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
