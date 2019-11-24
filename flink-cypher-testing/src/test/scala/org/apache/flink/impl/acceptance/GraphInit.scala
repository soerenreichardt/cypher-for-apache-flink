package org.apache.flink.impl.acceptance

import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.support.creation.graphs.CAPFScanGraphFactory
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph

trait GraphInit {
  def initGraph(createQuery: String, additionalPatterns: Seq[Pattern] = Seq.empty)
    (implicit session: CAPFSession): RelationalCypherGraph[FlinkTable]
}

trait ScanGraphInit extends GraphInit {
  def initGraph(createQuery: String, additionalPatterns: Seq[Pattern] = Seq.empty)
    (implicit session: CAPFSession): RelationalCypherGraph[FlinkTable] = {
    CAPFScanGraphFactory.iniGraph(createQuery, additionalPatterns)
  }
}
