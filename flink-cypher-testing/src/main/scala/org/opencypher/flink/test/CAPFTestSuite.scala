package org.opencypher.flink.test

import org.opencypher.flink.impl.physical.CAPFRuntimeContext
import org.opencypher.flink.test.fixture.{CAPFSessionFixture, FlinkSessionFixture}
import org.opencypher.flink.test.support.{GraphMatchingTestSupport, RecordMatchingTestSupport}
import org.opencypher.okapi.testing.BaseTestSuite

abstract class CAPFTestSuite
  extends BaseTestSuite
  with FlinkSessionFixture
  with CAPFSessionFixture
  with GraphMatchingTestSupport
  with RecordMatchingTestSupport {

  implicit val context: CAPFRuntimeContext = CAPFRuntimeContext.empty
}
