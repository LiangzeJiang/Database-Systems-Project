package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  protected var scan_table: IndexedSeq[IndexedSeq[Elem]] = IndexedSeq()
  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    if (getRowType.getFieldCount > 0) {
      val size_col = scannable.getColumn(0).size
      for (i <- 0 until getRowType.getFieldCount) {
        scan_table = scan_table :+ scannable.getColumn(i).toIndexedSeq
      }
      scan_table = scan_table :+ IndexedSeq.fill(size_col)(true)
    }
    scan_table
  }
}
