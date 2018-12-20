package org.opencypher.flink.test.fixture

import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.support.capf.{CAPFScanGraphFactory, CAPFTestGraphFactory}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory
import org.opencypher.flink.impl.CAPFConverters._

trait GraphConstructionFixture {
  self: CAPFSessionFixture with BaseTestSuite =>

  def capfGraphFacttory: CAPFTestGraphFactory = CAPFScanGraphFactory

  val initGraph: String => RelationalCypherGraph[FlinkTable] =
    createQuery => CAPFScanGraphFactory(CreateGraphFactory(createQuery)).asCapf
}
