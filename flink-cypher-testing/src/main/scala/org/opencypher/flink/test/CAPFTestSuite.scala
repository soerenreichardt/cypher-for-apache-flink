package org.opencypher.flink.test

import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.fixture.{CAPFSessionFixture, FlinkSessionFixture}
import org.opencypher.flink.test.support.{GraphMatchingTestSupport, RecordMatchingTestSupport}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.testing.BaseTestSuite

abstract class CAPFTestSuite
  extends BaseTestSuite
  with FlinkSessionFixture
  with CAPFSessionFixture
  with GraphMatchingTestSupport
  with RecordMatchingTestSupport {

  def catalog(qgn: QualifiedGraphName): Option[RelationalCypherGraph[FlinkTable]] = None

  implicit val context: RelationalRuntimeContext[FlinkTable] = RelationalRuntimeContext(catalog)
}
