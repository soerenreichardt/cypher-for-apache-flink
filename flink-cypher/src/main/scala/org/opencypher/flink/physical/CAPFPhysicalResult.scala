package org.opencypher.flink.physical

import org.opencypher.flink.{CAPFGraph, CAPFRecords, CAPFSession}
import org.opencypher.okapi.relational.api.physical.PhysicalResult

object CAPFPhysicalResult {
  def unit(implicit capf: CAPFSession) = CAPFPhysicalResult(CAPFRecords.unit(), Map.empty)
}

case class CAPFPhysicalResult( records: CAPFRecords, graphs: Map[String, CAPFGraph])
  extends PhysicalResult[CAPFRecords , CAPFGraph] {
  /**
    * Performs the given function on the underlying records and returns the updated records.
    *
    * @param f map function
    * @return updated result
    */
  override def mapRecordsWithDetails(f: CAPFRecords => CAPFRecords): CAPFPhysicalResult =
    copy(records = f(records))

  /**
    * Stores the given graph identifed by the specified name in the result.
    *
    * @param t tuple mapping a graph name to a graph
    * @return updated result
    */
  override def withGraph(t: (String, CAPFGraph)): CAPFPhysicalResult =
    copy(graphs = graphs.updated(t._1, t._2))

  /**
    * Returns a result that only contains the graphs with the given names.
    *
    * @param selected graphs to select
    * @return updated result containing only selected graphs
    */
  override def selectGraphs(selected: Set[String]): CAPFPhysicalResult =
    copy(graphs = graphs.filterKeys(selected))
}
