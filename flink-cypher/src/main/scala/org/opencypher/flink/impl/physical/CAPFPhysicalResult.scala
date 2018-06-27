package org.opencypher.flink.impl.physical

import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.api.physical.PhysicalResult

case class CAPFPhysicalResult(
  records: CAPFRecords,
  workingGraph: CAPFGraph,
  workingGraphName: QualifiedGraphName,
  tagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = Map.empty
)
  extends PhysicalResult[FlinkTable, CAPFRecords, CAPFGraph] {

  override def mapRecordsWithDetails(f: CAPFRecords => CAPFRecords): CAPFPhysicalResult =
    copy(records = f(records))
}
