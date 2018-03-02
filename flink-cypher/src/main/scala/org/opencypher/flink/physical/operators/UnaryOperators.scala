package org.opencypher.flink.physical.operators

import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.{CAPFRecords, ColumnName}
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.TableOps._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.syntax.ExprSyntax._
import org.opencypher.okapi.logical.impl.{LogicalExternalGraph, LogicalGraph}
import org.opencypher.okapi.relational.impl.table.{FieldSlotContent, ProjectedExpr, RecordHeader}

private[flink] abstract class UnaryPhysicalOperator extends CAPFPhysicalOperator {

  def in: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Cache(in: CAPFPhysicalOperator) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    context.cache.getOrElse(in, {
      prev.records.cache()
      context.cache(in) = prev
      prev
    })
  }
}

final case class Scan(in: CAPFPhysicalOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val graphs = prev.graphs
    val graph = graphs(inGraph.name)
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    CAPFPhysicalResult(records, graphs)
  }

}

final case class Alias(in: CAPFPhysicalOperator, expr: Expr, alias: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val oldSlot = records.header.slotsFor(expr).head

      val newSlot = header.slotsFor(alias).head

      val oldColumnName = ColumnName.of(oldSlot)
      val newColumnName = ColumnName.of(newSlot)

      val newData = if (records.data.columns.contains(oldColumnName)) {
        records.data.safeRenameColumn(oldColumnName, newColumnName)
      } else {
        throw IllegalArgumentException(s"a column with name $oldColumnName")
      }

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
  }
}

final case class SelectFields(in: CAPFPhysicalOperator, field: IndexedSeq[Var], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val fieldIndices = field.zipWithIndex.toMap

      val groupedSlots = header.slots.sortBy {
        _.content match {
          case content: FieldSlotContent =>
            fieldIndices.getOrElse(content.field, Int.MaxValue)
          case content@ProjectedExpr(expr) =>
            val deps = expr.dependencies
            deps.headOption
              .filter(_ => deps.size == 1)
              .flatMap(fieldIndices.get)
              .getOrElse(Int.MaxValue)
        }
      }

      val data = records.data
      val columns = groupedSlots.map { s =>
        UnresolvedFieldReference(ColumnName.of(s))
      }
      val newData = records.data.select(columns: _*)

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
  }
}

final case class SelectGraphs(in: CAPFPhysicalOperator, graphs: Set[String])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    prev.selectGraphs(graphs)

}

final case class SetSourceGraph(in: CAPFPhysicalOperator, graph: LogicalExternalGraph) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    prev.withGraph(graph.name -> resolve(graph.qualifiedGraphName))
}
