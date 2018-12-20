package org.opencypher.flink.test.support

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.BatchTableEnvironment
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.fixture.{CAPFSessionFixture, FlinkSessionFixture}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.testing.BaseTestSuite
import org.scalatest.Assertion

trait GraphMatchingTestSupport {

  self: BaseTestSuite with FlinkSessionFixture with CAPFSessionFixture =>

  val env: ExecutionEnvironment = sessionEnv
  val tableEnv: BatchTableEnvironment = sessionTableEnv

  private def getEntityIds(records: RelationalCypherRecords[FlinkTable]): Set[Long] = {
    val entityVar = records.header.vars.toSeq match {
      case Seq(v) => v
      case other => throw new UnsupportedOperationException(s"Expected records with 1 entity, got $other")
    }

    records.table.table.select(records.header.column(entityVar)).collect().map(_.getField(0).asInstanceOf[Long]).toSet
  }

  private def verify(actual: RelationalCypherGraph[FlinkTable], expected: RelationalCypherGraph[FlinkTable]): Assertion = {
    val expectedNodeIds = getEntityIds(expected.nodes("n"))
    val expectedRelIds = getEntityIds(expected.relationships("r"))
    val actualNodeIds = getEntityIds(actual.nodes("n"))
    val actualRelIds = getEntityIds(actual.relationships("r"))

    expectedNodeIds should equal(actualNodeIds)
    expectedRelIds should equal(actualRelIds)
  }

  implicit class GraphsMatcher(graphs: Map[String, RelationalCypherGraph[FlinkTable]]) {
    def shouldMatch(expectedGraphs: RelationalCypherGraph[FlinkTable]*): Unit = {
      withClue("expected and actual must have same size") {
        graphs.size should equal(expectedGraphs.size)
      }

      graphs.values.zip(expectedGraphs).foreach {
        case (actual, expected) => verify(actual, expected)
      }
    }
  }

  implicit class GraphMatcher(graph: RelationalCypherGraph[FlinkTable]) {
    def shouldMatch(expectedGraph: RelationalCypherGraph[FlinkTable]): Unit = verify(graph, expectedGraph)
  }

}
