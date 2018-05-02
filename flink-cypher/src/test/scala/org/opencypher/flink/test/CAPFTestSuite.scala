package org.opencypher.flink.test

import org.opencypher.flink.physical.CAPFRuntimeContext
import org.opencypher.flink.test.fixture.{CAPFSessionFixture, FlinkSessionFixture}
import org.opencypher.flink.test.support.{GraphMatchingTestSupport, RecordMatchingTestSupport, RowDebugOutputSupport}
import org.opencypher.okapi.test.BaseTestSuite

abstract class CAPFTestSuite
  extends BaseTestSuite
  with FlinkSessionFixture
  with CAPFSessionFixture
  with GraphMatchingTestSupport
  with RecordMatchingTestSupport
  with RowDebugOutputSupport {

  implicit val context: CAPFRuntimeContext = CAPFRuntimeContext.empty
}
