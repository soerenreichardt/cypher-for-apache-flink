package org.opencypher.flink.test.fixture

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment}
import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.okapi.test.fixture.BaseTestFixture

trait FlinkSessionFixture extends BaseTestFixture {
  self: BaseTestSuite =>

  implicit val sessionEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  implicit val sessionTableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(sessionEnv)
}
