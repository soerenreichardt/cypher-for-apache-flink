package org.opencypher.flink.impl.graph

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory}

case class CAPFGraphFactory(implicit val session: CAPFSession) extends RelationalCypherGraphFactory[FlinkTable] {
  override type Graph = RelationalCypherGraph[FlinkTable]
}
