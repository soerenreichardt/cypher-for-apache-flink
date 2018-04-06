package org.opencypher.flink.physical.operators

import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.CAPFRecords
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.physical.operators.CAPFPhysicalOperator._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.TableOps._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class BinaryPhysicalOperator extends CAPFPhysicalOperator {

  def lhs: CAPFPhysicalOperator

  def rhs: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Join(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  joinColumns: Seq[(Expr, Expr)],
  header: RecordHeader,
  joinType: String)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val joinSlots = joinColumns.map {
      case (leftExpr, rightExpr) =>
        val leftRecordSlot = header.slotsFor(leftExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", leftExpr))
        val rightRecordSlot = header.slotsFor(rightExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", rightExpr))

        leftRecordSlot -> rightRecordSlot
    }

    val joinedRecords = joinRecords(header, joinSlots, joinType)(left.records, right.records)

    CAPFPhysicalResult(joinedRecords, left.graphs)
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

final case class CartesianProduct(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val data = left.records.data
    val otherData = right.records.data
    val newData = data.cross(otherData)(left.records.capf)
    val records = CAPFRecords.verifyAndCreate(header, newData)(left.records.capf)
    val graphs = left.graphs ++ right.graphs
    CAPFPhysicalResult(records, graphs)
  }
}