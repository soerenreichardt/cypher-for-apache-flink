package org.opencypher.flink.test.support.creation.graphs

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory}
import org.opencypher.flink.impl.CAPFConverters._

trait TestGraphFactory extends CypherTestGraphFactory[CAPFSession] {
  def iniGraph(createQuery: String, additionalPatterns: Seq[Pattern] = Seq.empty)
    (implicit session: CAPFSession): RelationalCypherGraph[FlinkTable] = {
    apply(CreateGraphFactory(createQuery), additionalPatterns).asCapf
  }

}
