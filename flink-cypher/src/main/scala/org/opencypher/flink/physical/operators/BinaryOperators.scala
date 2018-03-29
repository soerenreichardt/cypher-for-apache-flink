package org.opencypher.flink.physical.operators

import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.CAPFRecords
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.physical.operators.CAPFPhysicalOperator._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class BinaryPhysicalOperator extends CAPFPhysicalOperator {

  def lhs: CAPFPhysicalOperator

  def rhs: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class ValueJoin(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  predicates: Set[org.opencypher.okapi.ir.api.expr.Equals],
  header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(
    implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftHeader = left.records.header
    val rightHeader = right.records.header
    val slots = predicates.map { p =>
      leftHeader.slotsFor(p.lhs).head -> rightHeader.slotsFor(p.rhs).head
    }.toSeq

    CAPFPhysicalResult(joinRecords(header, slots)(left.records, right.records), left.graphs ++ right.graphs)
  }
}

final case class Union(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator)
  extends BinaryPhysicalOperator with InheritedHeader {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftData = left.records.data
    val rightData = right.records.data.select(leftData.columns.map(UnresolvedFieldReference): _*)

    val unionedData = leftData.union(rightData)
    val records = CAPFRecords.verifyAndCreate(header, unionedData)(left.records.capf)

    CAPFPhysicalResult(records, left.graphs ++ right.graphs)
  }

}