package org.opencypher.flink.impl.physical.operators

import org.apache.flink.table.api.Table
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink._
import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
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
  with PhysicalOperator[FlinkTable, CAPFRecords, CAPFGraph, CAPFRuntimeContext] {

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

