package org.opencypher.flink.test.support.capf

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory}

trait CAPFTestGraphFactory extends CypherTestGraphFactory[CAPFSession] {
  def initGraph(createQuery: String)(implicit capf: CAPFSession): RelationalCypherGraph[FlinkTable] = {
    apply(CreateGraphFactory(createQuery)).asCapf
  }
}
