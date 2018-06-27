package org.opencypher.flink.impl.physical

import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.physical.operators.CAPFPhysicalOperator
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.physical.RuntimeContext

import scala.collection.mutable

object CAPFRuntimeContext {
  val empty = CAPFRuntimeContext(CypherMap.empty,  _ => None, mutable.Map.empty, mutable.Map.empty)
}

case class CAPFRuntimeContext(
  parameters: CypherMap,
  resolve: QualifiedGraphName => Option[CAPFGraph],
  cache: mutable.Map[CAPFPhysicalOperator, CAPFPhysicalResult],
  patternGraphTags: mutable.Map[QualifiedGraphName, Set[Int]])
extends RuntimeContext[FlinkTable, CAPFRecords , CAPFGraph]
