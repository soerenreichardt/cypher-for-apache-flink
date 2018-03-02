package org.opencypher.flink.physical.operators

import org.opencypher.flink.{CAPFRecords, CAPFSession}
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.logical.impl.LogicalExternalGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class LeafPhysicalOperator extends CAPFPhysicalOperator {

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeLeaf()

  def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Start(records: CAPFRecords, graph: LogicalExternalGraph) extends LeafPhysicalOperator {

  override def header: RecordHeader = records.header

  override def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    CAPFPhysicalResult(records, Map(graph.name -> resolve(graph.qualifiedGraphName)))
}

final case class StartFromUnit(graph: LogicalExternalGraph)(implicit capf: CAPFSession)
  extends LeafPhysicalOperator {

  override val header = RecordHeader.empty

  override def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    CAPFPhysicalResult(CAPFRecords.unit(), Map(graph.name -> resolve(graph.qualifiedGraphName)))
}