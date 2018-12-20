package org.opencypher.flink.test.fixture

import org.opencypher.flink.api.CAPFSession
import org.opencypher.okapi.testing.{BaseTestFixture, BaseTestSuite}

trait CAPFSessionFixture extends BaseTestFixture {
  self: FlinkSessionFixture with BaseTestSuite =>

  implicit lazy val capf: CAPFSession = CAPFSession.local()

  abstract override protected def afterEach(): Unit = {
    capf.catalog.source(capf.catalog.sessionNamespace).graphNames.map(_.value).foreach(capf.catalog.dropGraph)
    capf.catalog.store(capf.emptyGraphQgn, capf.graphs.empty)
    super.afterEach()
  }

}
