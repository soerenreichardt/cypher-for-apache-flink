package org.opencypher.flink.test.support

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.BatchTableEnvironment
import org.opencypher.flink.impl.CAPFGraph
import org.opencypher.flink.test.fixture.{CAPFSessionFixture, FlinkSessionFixture}
import org.opencypher.okapi.testing.BaseTestSuite
import org.scalatest.Assertion

trait GraphMatchingTestSupport {

  self: BaseTestSuite with FlinkSessionFixture with CAPFSessionFixture =>

  val env: ExecutionEnvironment = sessionEnv
  val tableEnv: BatchTableEnvironment = sessionTableEnv

  private def verify(actual: CAPFGraph, expected: CAPFGraph): Assertion = {
    val expectedNodeIds = expected.nodes("n").table.select("n").collect().map(_.getField(0).asInstanceOf[Long]).toSet
    val expectedRelIds = expected.relationships("r").table.select("r").collect().map(_.getField(0).asInstanceOf[Long]).toSet
    val actualNodeIds = actual.nodes("n").table.select("n").collect().map(_.getField(0).asInstanceOf[Long]).toSet
    val actualRelIds = actual.relationships("r").table.select("r").collect().map(_.getField(0).asInstanceOf[Long]).toSet

    expectedNodeIds should equal(actualNodeIds)
    expectedRelIds should equal(actualRelIds)
  }

  implicit class GraphsMatcher(graphs: Map[String, CAPFGraph]) {
    def shouldMatch(expectedGraphs: CAPFGraph*): Unit = {
      withClue("expected and actual must have same size") {
        graphs.size should equal(expectedGraphs.size)
      }

      graphs.values.zip(expectedGraphs).foreach {
        case (actual, expected) => verify(actual, expected)
      }
    }
  }

  implicit class GraphMatcher(graph: CAPFGraph) {
    def shouldMatch(expectedGraph: CAPFGraph): Unit = verify(graph, expectedGraph)
  }

}
