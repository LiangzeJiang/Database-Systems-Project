package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  /**
   * Helper function (you do not have to use it or implement it)
   * It's purpose is to show how to convert the [[scannable]] to a
   * specific [[Store]].
   *
   * @param rowId row number (startign from 0)
   * @return the row as a Tuple
   */
//  private def getRow(rowId: Int): Tuple = {
//    scannable match {
//      case rleStore: RLEStore =>
//        /**
//         * For this project, it's safe to assume scannable will always
//         * be a [[RLEStore]].
//         */
//        ???
//    }
//  }
  // self-defined parameters
  protected var curr_ind: Int = 0
  protected var table_len: Long = scannable.getRowCount
  protected var col_table: IndexedSeq[IndexedSeq[Any]] = IndexedSeq()
  protected var curr_col: IndexedSeq[Any] = IndexedSeq()
  protected var scan_table: IndexedSeq[Tuple] = IndexedSeq()
  protected var scan_size: Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    scan_table = scannable match {
      case rleStore: RLEStore => {
        if (getRowType.getFieldCount > 0) {
          for (i <- 0 until getRowType.getFieldCount) {
            // set curr_col as empty
            var curr_col = IndexedSeq[Any]()
            for (entry <- rleStore.getRLEColumn(i)) {
              for (_ <- 0 until entry.length.toInt) curr_col = curr_col ++ entry.value
//              curr_col = curr_col ++ {for (_ <- 0 until entry.length.toInt) yield entry.value.head} // IndexedSeq[Tuple]
            }
            col_table = col_table :+ curr_col
          }
        }
        col_table.transpose
      }
    }
    scan_size = scan_table.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (curr_ind < scan_size) {
      curr_ind += 1
      Option(scan_table(curr_ind - 1))
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}


//  /**
//    * @inheritdoc
//    */
//  override def next(): Option[Tuple] = {
//    if (curr_ind < table_len) {
//      val new_tuple: Tuple = scannable match {
//        case rleStore: RLEStore => rleStore.
//      }
//      curr_ind = curr_ind + 1
//      Option(new_tuple)
//    } else {
//      NilTuple
//    }
//  }
//
//  /**
//    * @inheritdoc
//    */
//  override def close(): Unit = {}
}
