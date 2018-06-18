package org.opencypher.flink.impl.physical.operators

import org.apache.flink.table.api.Table
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink._
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords, CAPFSession}
import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode

private[flink] abstract class CAPFPhysicalOperator
  extends AbstractTreeNode[CAPFPhysicalOperator]
  with PhysicalOperator[CAPFRecords, CAPFGraph, CAPFRuntimeContext] {

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: CAPFRuntimeContext): CAPFGraph = {
    context.resolve(qualifiedGraphName).map(_.asCapf).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: CAPFRuntimeContext): Set[Int] = context.patternGraphTags.getOrElse(qgn, resolve(qgn).tags)

  override def args: Iterator[Any] = super.args.flatMap {
    case RecordHeader | Some(RecordHeader)  => None
    case other                              => Some(other)
  }
}

object CAPFPhysicalOperator {
  def joinRecords(
    header: RecordHeader,
    joinColumns: Seq[(String, String)],
    joinType: String = "inner",
    deduplicate: Boolean = false)(lhs: CAPFRecords, rhs: CAPFRecords): CAPFRecords = {

    val lhsData = lhs.toTable()
    val rhsData = rhs.toTable()

    joinTables(lhsData, rhsData,  header, joinColumns, joinType)(deduplicate)(lhs.capf)
  }

  def joinTables(lhsData: Table, rhsData: Table, header: RecordHeader, joinCols: Seq[(String, String)], joinType: String)
    (deduplicate: Boolean)(implicit capf: CAPFSession): CAPFRecords = {

    val joinedData = lhsData.safeJoin(rhsData, joinCols, joinType)

    val returnData = if (deduplicate) {
      val colsToDrop = joinCols.map(col => col._2)
      joinedData.safeDropColumns(colsToDrop: _*)
    } else joinedData

    CAPFRecords(header, returnData)
  }

  def assertIsNode(e: Expr): Unit = {
    e.cypherType match {
      case CTNode(_, _) =>
      case x =>
        throw IllegalArgumentException(s"Expected $e to contain a node, but was $x")
    }
  }
}
