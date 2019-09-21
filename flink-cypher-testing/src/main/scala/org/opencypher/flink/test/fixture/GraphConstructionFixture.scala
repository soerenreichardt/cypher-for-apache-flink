package org.opencypher.flink.test.fixture

import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.support.capf.CAPFScanGraphFactory
import org.opencypher.flink.test.support.creation.graphs.TestGraphFactory
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory
import org.opencypher.flink.impl.CAPFConverters._

trait GraphConstructionFixture {
  self: CAPFSessionFixture with BaseTestSuite =>

  def graphFactory: TestGraphFactory = CAPFScanGraphFactory

  def initGraph(query: String, additionalPatterns: Seq[Pattern] = Seq.empty): RelationalCypherGraph[FlinkTable] =
    CAPFScanGraphFactory(CreateGraphFactory(query), additionalPatterns).asCapf
}
