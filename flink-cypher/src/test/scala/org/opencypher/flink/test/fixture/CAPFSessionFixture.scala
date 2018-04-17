package org.opencypher.flink.test.fixture

import org.opencypher.flink.CAPFSession
import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.okapi.test.fixture.BaseTestFixture

trait CAPFSessionFixture extends BaseTestFixture {
  self: FlinkSessionFixture with BaseTestSuite =>

  implicit lazy val capf: CAPFSession = CAPFSession.create()

  abstract override protected def afterEach(): Unit = {
    capf.dataSource(capf.sessionNamespace).graphNames.foreach(capf.delete)
    super.afterEach()
  }

}
