package org.opencypher.flink.physical.operators

import org.opencypher.flink.{CAPFRecords, CAPFUnionGraph}
import org.opencypher.flink.physical.CAPFRuntimeContext
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.flink.TagSupport._
import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}

final case class GraphUnionAll(inputs: List[CAPFPhysicalOperator], qgn: QualifiedGraphName)
  extends CAPFPhysicalOperator with InheritedHeader {
  require(inputs.nonEmpty, "GraphUnionAll requires at least one input")

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val inputResults = inputs.map(_.execute)
    implicit val capf = inputResults.head.records.capf

    val graphTags = inputResults.map(r => r.workingGraphName -> r.workingGraph.tags).toMap
    val tagStrategy = computeRetaggings(graphTags)
    val graphWithTagStrategy = inputResults.map(r => r.workingGraph -> tagStrategy(r.workingGraphName)).toMap

    val unionGraph = CAPFUnionGraph(graphWithTagStrategy)

    CAPFPhysicalResult(CAPFRecords.unit(), unionGraph, qgn, tagStrategy)
  }
}