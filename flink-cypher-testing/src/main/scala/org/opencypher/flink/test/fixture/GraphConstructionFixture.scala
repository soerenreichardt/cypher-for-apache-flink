package org.opencypher.flink.test.fixture

import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.CAPFGraph
import org.opencypher.flink.test.support.capf.{CAPFScanGraphFactory, CAPFTestGraphFactory}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory

trait GraphConstructionFixture {
  self: CAPFSessionFixture with BaseTestSuite =>

  def capfGraphFacttory: CAPFTestGraphFactory = CAPFScanGraphFactory

  val initGraph: String => CAPFGraph =
    (createQuery) => CAPFScanGraphFactory(CreateGraphFactory(createQuery)).asCapf
}
