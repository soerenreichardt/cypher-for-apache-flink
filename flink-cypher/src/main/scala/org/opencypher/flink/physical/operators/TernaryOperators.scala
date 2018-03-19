package org.opencypher.flink.physical.operators

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.expressions.{Expression, UnresolvedFieldReference}
import org.opencypher.flink.CAPFRecords
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

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

    print(first.records.data.collect())
    print(second.records.data.collect())
    print(third.records.data.collect())

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