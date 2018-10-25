package org.opencypher.flink.impl.graph

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.api.io.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.impl.graph.{ScanGraph, SingleTableGraph}
import org.opencypher.okapi.relational.impl.operators.Start
import org.opencypher.flink.impl.CAPFConverters._

case class CAPFGraphFactory(implicit val session: CAPFSession) extends RelationalCypherGraphFactory[FlinkTable] {

  override type Graph = RelationalCypherGraph[FlinkTable]

  def create(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*): Graph = {
    create(Set(0), None, nodeTable, entityTables: _*)
  }

  def create(maybeSchema: Option[CAPFSchema], nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*): Graph = {
    create(Set(0), maybeSchema, nodeTable, entityTables: _*)
  }

  def create(
    tags: Set[Int],
    maybeSchema: Option[CAPFSchema],
    nodeTable: CAPFNodeTable,
    entityTables: CAPFEntityTable*
  ): Graph = {
    implicit val runtimeContext: RelationalRuntimeContext[FlinkTable] = session.basicRuntimeContext()
    val allTables = nodeTable +: entityTables
    val schema = maybeSchema.getOrElse(allTables.map(_.schema).reduce[Schema](_ ++ _).asCapf)
    new ScanGraph(allTables, schema, tags)
  }

  def create(records: CypherRecords, schema: CAPFSchema, tags: Set[Int] = Set(0)): Graph = {
    implicit val runtimeContext: RelationalRuntimeContext[FlinkTable] = session.basicRuntimeContext()
    val capfRecords = records.asCapf
    new SingleTableGraph(Start(capfRecords), schema, tags)
  }

}
