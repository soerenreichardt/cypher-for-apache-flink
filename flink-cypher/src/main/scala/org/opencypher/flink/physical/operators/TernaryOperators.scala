package org.opencypher.flink.physical.operators

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, In, UnresolvedFieldReference}
import org.opencypher.flink.CAPFRecords
import org.opencypher.flink.TableOps._
import org.opencypher.flink.physical.operators.CAPFPhysicalOperator._
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.expr.{EndNode, Var}
import org.opencypher.okapi.logical.impl.{Directed, Direction, Undirected}
import org.opencypher.okapi.relational.impl.table.{OpaqueField, ProjectedExpr, RecordHeader, RecordSlot}

private[flink] abstract class TernaryPhysicalOperator extends CAPFPhysicalOperator {

  def first: CAPFPhysicalOperator

  def second: CAPFPhysicalOperator

  def third: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    executeTernary(first.execute, second.execute, third.execute)

  def executeTernary(first: CAPFPhysicalResult, second: CAPFPhysicalResult, third: CAPFPhysicalResult)(
    implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class ExpandSource(
  first: CAPFPhysicalOperator,
  second: CAPFPhysicalOperator,
  third: CAPFPhysicalOperator,
  source: Var,
  rel: Var,
  target: Var,
  header: RecordHeader,
  removeSelfRelationships: Boolean = false)
  extends TernaryPhysicalOperator {

  override def executeTernary(first: CAPFPhysicalResult, second: CAPFPhysicalResult, third: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val relationships = getRelationshipData(second.records)

    val sourceSlot = first.records.header.slotFor(source)
    val sourceSlotInRel = second.records.header.sourceNodeSlot(rel)
    CAPFPhysicalOperator.assertIsNode(sourceSlot)
    CAPFPhysicalOperator.assertIsNode(sourceSlotInRel)

    val sourceToRelHeader = first.records.header ++ second.records.header
    val sourceAndRel = CAPFPhysicalOperator.joinRecords(sourceToRelHeader, Seq(sourceSlot -> sourceSlotInRel))(first.records, relationships)

    val targetSlot = third.records.header.slotFor(target)
    val targetSlotInRel = sourceAndRel.header.targetNodeSlot(rel)
    CAPFPhysicalOperator.assertIsNode(targetSlot)
    CAPFPhysicalOperator.assertIsNode(targetSlotInRel)

    val joinedRecords = CAPFPhysicalOperator.joinRecords(header, Seq(targetSlotInRel -> targetSlot))(sourceAndRel, third.records)
    CAPFPhysicalResult(joinedRecords, first.graphs ++ second.graphs ++ third.graphs)
  }

  private def getRelationshipData(rels: CAPFRecords)(implicit context: CAPFRuntimeContext): CAPFRecords = {
    if (removeSelfRelationships) {
      val data = rels.data
      val startNodeColumn: Expression = UnresolvedFieldReference(CAPFPhysicalOperator.columnName(rels.header.sourceNodeSlot(rel)))
      val endNodeColumn: Expression = UnresolvedFieldReference(CAPFPhysicalOperator.columnName(rels.header.targetNodeSlot(rel)))

      CAPFRecords.verifyAndCreate(rels.header, data.where(endNodeColumn !== startNodeColumn))(rels.capf)
    } else rels
  }
}

final case class BoundedVarExpand(
  first: CAPFPhysicalOperator,
  second: CAPFPhysicalOperator,
  third: CAPFPhysicalOperator,
  rel: Var,
  edgeList: Var,
  target: Var,
  initialEndNode: Var,
  lower: Int,
  upper: Int,
  direction: Direction,
  header: RecordHeader,
  isExpandInto: Boolean)
  extends TernaryPhysicalOperator {

  override def executeTernary(first: CAPFPhysicalResult, second: CAPFPhysicalResult, third: CAPFPhysicalResult)
    (implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val expanded = expand(first.records, second.records)

    CAPFPhysicalResult(finalize(expanded, third.records), first.graphs ++ second.graphs ++ third.graphs)
  }

  private def iterate(lhs: Table, rels: Table)(
    endNode: RecordSlot,
    rel: Var,
    relStartNode: RecordSlot,
    listTempColName: String,
    edgeListColName: String,
    keep: Array[String]): Table = {

    val relIdColumnName = columnName(OpaqueField(rel))
    val startColumnName = columnName(relStartNode)
    val expandColumnName = columnName(endNode)

    val joined = lhs.join(rels, expandColumnName === startColumnName)

    val extendedArray = Seq(edgeListColName, relIdColumnName)
    val withExtendedArray = joined
      .safeAddColumn(listTempColName, array(extendedArray.head, extendedArray.tail.map(UnresolvedFieldReference): _*))
    val arrayContains = In(relIdColumnName, extendedArray.map(UnresolvedFieldReference))
    val filtered = withExtendedArray.filter(!arrayContains)

    val endNodeIdColNameOfJoinedRel = columnName(ProjectedExpr(EndNode(rel)(CTNode)))

    val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
    val withoutRelProperties = filtered.select(columns.map(UnresolvedFieldReference).toSeq: _*)

    withoutRelProperties
      .safeDropColumn(expandColumnName)
      .safeRenameColumn(endNodeIdColNameOfJoinedRel, expandColumnName)
      .safeDropColumn(edgeListColName)
      .safeRenameColumn(listTempColName, edgeListColName)
  }

  private def finalize(expanded: CAPFRecords, targets: CAPFRecords): CAPFRecords = {
    val endNodeSlot = expanded.header.slotFor(initialEndNode)
    val endNodeCol = columnName(endNodeSlot)

    val targetNodeSlot = targets.header.slotFor(target)
    val targetNodeCol = columnName(targetNodeSlot)

    val result = if (isExpandInto) {
      val data = expanded.toTable()
      CAPFRecords.verifyAndCreate(header, data.filter(targetNodeCol === endNodeCol))(expanded.capf)
    } else {
      val joinHeader = expanded.header ++ targets.header

      val lhsSlot = expanded.header.slotFor(initialEndNode)
      val rhsSlot = targets.header.slotFor(target)

      assertIsNode(lhsSlot)
      assertIsNode(rhsSlot)

      joinRecords(joinHeader, Seq(lhsSlot -> rhsSlot))(expanded, targets)
    }

    CAPFRecords.verifyAndCreate(header, result.toTable().safeDropColumn(endNodeCol))(expanded.capf)
  }

  private def expand(firstRecords: CAPFRecords, secondRecords: CAPFRecords): CAPFRecords = {
    val initData = firstRecords.data
    val relsData = direction match {
      case Directed =>
        secondRecords.data
      case Undirected =>
        val startNodeSlot = columnName(secondRecords.header.sourceNodeSlot(rel))
        val endNodeSlot = columnName(secondRecords.header.targetNodeSlot(rel))
        val colOrder = secondRecords.header.slots.map(columnName)

        val inverted = secondRecords.data
          .safeRenameColumn(startNodeSlot, "__tmp__")
          .safeRenameColumn(endNodeSlot, startNodeSlot)
          .safeRenameColumn("__tmp__", endNodeSlot)
          .select(colOrder.map(UnresolvedFieldReference): _*)

        inverted.union(secondRecords.data)
    }

    val edgeListColName = columnName(firstRecords.header.slotFor(edgeList))

    val steps = new collection.mutable.HashMap[Int, Table]
    steps(0) = initData

    val keep = initData.getSchema.getColumnNames

    val listTempColName =
      ColumnNameGenerator.generateUniqueName(firstRecords.header)

    val startSlot = secondRecords.header.sourceNodeSlot(rel)
    val endNodeSlot = firstRecords.header.slotFor(initialEndNode)
    (1 to upper).foreach { i =>
      steps(i) = iterate(steps(i-1), relsData)(endNodeSlot, rel, startSlot, listTempColName, edgeListColName, keep)
    }

    val union = steps.filterKeys(_ >= lower).values.reduce[Table] {
      case (l, r) => l.union(r)
    }

    CAPFRecords.verifyAndCreate(firstRecords.header, union)(firstRecords.capf)
  }
}