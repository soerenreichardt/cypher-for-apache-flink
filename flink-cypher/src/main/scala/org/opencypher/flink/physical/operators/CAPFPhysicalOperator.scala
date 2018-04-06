package org.opencypher.flink.physical.operators

import org.apache.flink.table.api.Table
import org.opencypher.flink.CAPFConverters._
import org.opencypher.flink.TableOps._
import org.opencypher.flink._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.relational.impl.table.{RecordHeader, RecordSlot, SlotContent}
import org.opencypher.okapi.trees.AbstractTreeNode

private[flink] abstract class CAPFPhysicalOperator
  extends AbstractTreeNode[CAPFPhysicalOperator]
  with PhysicalOperator[CAPFRecords, CAPFGraph, CAPFRuntimeContext] {

  override def header: RecordHeader

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: CAPFRuntimeContext): CAPFGraph = {
    context.resolve(qualifiedGraphName).map(_.asCapf).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }

  override def args: Iterator[Any] = super.args.flatMap {
    case RecordHeader(_) | Some(RecordHeader(_)) => None
    case other                                   => Some(other)
  }
}

object CAPFPhysicalOperator {
  def columnName(slot: RecordSlot): String = ColumnName.of(slot)

  def columnName(content: SlotContent): String = ColumnName.of(content)

  def joinRecords(
    header: RecordHeader,
    joinSlots: Seq[(RecordSlot, RecordSlot)],
    joinType: String = "inner",
    deduplicate: Boolean = false)(lhs: CAPFRecords, rhs: CAPFRecords): CAPFRecords = {

    val lhsData = lhs.toTable()
    val rhsData = rhs.toTable()

    val joinCols = joinSlots.map(pair => columnName(pair._1) -> columnName(pair._2))

    joinTables(lhsData, rhsData,  header, joinCols, joinType)(deduplicate)(lhs.capf)
  }

  def joinTables(lhsData: Table, rhsData: Table, header: RecordHeader, joinCols: Seq[(String, String)], joinType: String)
    (deduplicate: Boolean)(implicit capf: CAPFSession): CAPFRecords = {

    val joinedData = lhsData.safeJoin(rhsData, joinCols, joinType)

    val returnData = if (deduplicate) {
      val colsToDrop = joinCols.map(col => col._2)
      joinedData.safeDropColumns(colsToDrop: _*)
    } else joinedData

    CAPFRecords.verifyAndCreate(header, returnData)
  }

  def assertIsNode(slot: RecordSlot): Unit = {
    slot.content.cypherType match {
      case CTNode(_) =>
      case x =>
        throw IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
    }
  }
}
